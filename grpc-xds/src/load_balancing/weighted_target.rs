/*
 *
 * Copyright 2026 gRPC authors.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 *
 */

//! Implementation of the `weighted_target_experimental` Load Balancing policy.
//!
//! Specifications:
//! - [gRFC A28: xDS Traffic Splitting and Routing](https://github.com/grpc/proposal/blob/master/A28-xds-traffic-splitting-and-routing.md#weighted-target-lb-policy)
//! - [gRFC A56: Priority LB Policy](https://github.com/grpc/proposal/blob/master/A56-priority-lb-policy.md#hierarchical-addresses)
//! - [gRFC A78: gRPC Metrics: WRR, Pick First, and xDS](https://github.com/grpc/proposal/blob/master/A78-grpc-metrics-wrr-pf-xds.md#weighted-target-lb-policy)
//!
//! The Weighted Target LB policy manages a collection of weighted child
//! policies. When the aggregate state is READY or TRANSIENT_FAILURE, picks are
//! distributed across the children in that state in proportion to their
//! configured weights.

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;
use std::sync::Once;

use grpc::__unstable::client::load_balancing::ChannelController;
use grpc::__unstable::client::load_balancing::LbConfigJson;
use grpc::__unstable::client::load_balancing::LbPolicy;
use grpc::__unstable::client::load_balancing::LbPolicyBuilder;
use grpc::__unstable::client::load_balancing::LbPolicyOptions;
use grpc::__unstable::client::load_balancing::LbState;
use grpc::__unstable::client::load_balancing::ParsedLbConfig;
use grpc::__unstable::client::load_balancing::PickOptions;
use grpc::__unstable::client::load_balancing::PickResult;
use grpc::__unstable::client::load_balancing::Picker;
use grpc::__unstable::client::load_balancing::QueuingPicker;
use grpc::__unstable::client::load_balancing::WorkData;
use grpc::__unstable::client::load_balancing::child_manager::ChildManager;
use grpc::__unstable::client::load_balancing::child_manager::ChildUpdate;
use grpc::__unstable::client::load_balancing::endpoint_filtering;
use grpc::__unstable::client::load_balancing::registry::GLOBAL_LB_REGISTRY;
use grpc::__unstable::client::name_resolution::ResolverUpdate;
use grpc::StatusCodeError;
use grpc::StatusError;
use grpc::client::ConnectivityState;
use serde::Deserialize;

static POLICY_NAME: &str = "weighted_target_experimental";
static START: Once = Once::new();

/// Resolver attribute indicating the child target name / locality.
///
/// Defined in [gRFC A78](https://github.com/grpc/proposal/blob/master/A78-grpc-metrics-wrr-pf-xds.md#weighted-target-lb-policy).
#[derive(Debug, PartialEq, Eq)]
struct Locality(Arc<str>);

/// Sampler abstraction for generating random weight offsets.
trait RngSampler: Send + Sync + Debug {
    /// Generates a uniform random value in the half-open range `[0, total_weight)`.
    fn sample(&self, total_weight: u64) -> u64;
}

#[derive(Debug)]
struct DefaultRngSampler;

impl RngSampler for DefaultRngSampler {
    fn sample(&self, total_weight: u64) -> u64 {
        rand::random_range(0..total_weight)
    }
}

#[derive(Debug, Deserialize)]
struct TargetConfig {
    weight: u32,
    #[serde(rename = "childPolicy")]
    child_policy: ParsedLbConfig,
}

#[derive(Debug, Deserialize)]
struct WeightedTargetConfig {
    targets: BTreeMap<String, TargetConfig>,
}

#[derive(Debug)]
struct WeightedTargetBuilder {
    sampler: Arc<dyn RngSampler>,
}

impl Default for WeightedTargetBuilder {
    fn default() -> Self {
        Self {
            sampler: Arc::new(DefaultRngSampler),
        }
    }
}

impl WeightedTargetBuilder {
    #[cfg(test)]
    fn with_sampler(sampler: Arc<dyn RngSampler>) -> Self {
        Self { sampler }
    }
}

impl LbPolicyBuilder for WeightedTargetBuilder {
    type LbPolicy = WeightedTargetPolicy;

    fn build(&self, options: LbPolicyOptions) -> Self::LbPolicy {
        WeightedTargetPolicy::new(options, self.sampler.clone())
    }

    fn name(&self) -> &'static str {
        POLICY_NAME
    }

    fn parse_config(&self, config: &LbConfigJson) -> Result<WeightedTargetConfig, String> {
        let parsed: WeightedTargetConfig = config
            .convert_to()
            .map_err(|e| format!("{POLICY_NAME}: invalid config: {e}"))?;

        if parsed.targets.is_empty() {
            return Err(format!(
                "{POLICY_NAME}: invalid config: 'targets' must be non-empty"
            ));
        }

        for (target_name, target_cfg) in &parsed.targets {
            if target_cfg.weight == 0 {
                return Err(format!(
                    "{POLICY_NAME}: invalid config: target '{target_name}': weight must be greater than 0"
                ));
            }
        }

        Ok(parsed)
    }
}

#[derive(Debug)]
struct WeightedTargetPolicy {
    child_manager: ChildManager<String>,
    weights: HashMap<String, u32>,
    sampler: Arc<dyn RngSampler>,
}

impl WeightedTargetPolicy {
    fn new(options: LbPolicyOptions, sampler: Arc<dyn RngSampler>) -> Self {
        Self {
            child_manager: ChildManager::new(options.runtime, options.work_scheduler),
            weights: HashMap::new(),
            sampler,
        }
    }

    fn update_picker(&mut self, channel_controller: &mut dyn ChannelController) {
        let connectivity_state = self.child_manager.aggregate_states();
        let picker: Arc<dyn Picker> = match connectivity_state {
            ConnectivityState::Ready | ConnectivityState::TransientFailure => {
                let mut pickers = Vec::new();
                let mut cumulative_weight = 0u64;

                for child in self.child_manager.children() {
                    if child.state.connectivity_state == connectivity_state {
                        cumulative_weight += u64::from(self.weights[&child.identifier]);
                        pickers.push((cumulative_weight, child.state.picker.clone()));
                    }
                }

                Arc::new(WeightedPicker::new_with_sampler(
                    pickers,
                    self.sampler.clone(),
                ))
            }
            ConnectivityState::Connecting | ConnectivityState::Idle => Arc::new(QueuingPicker),
        };

        channel_controller.update_picker(LbState {
            connectivity_state,
            picker,
        });
    }
}

impl LbPolicy for WeightedTargetPolicy {
    type LbConfig = WeightedTargetConfig;

    fn resolver_update(
        &mut self,
        update: ResolverUpdate,
        config: &Self::LbConfig,
        channel_controller: &mut dyn ChannelController,
    ) -> Result<(), String> {
        let mut grouped_endpoints = update.endpoints.map(endpoint_filtering::group_by_path);

        let child_updates = config.targets.iter().map(|(target_name, target_config)| {
            let mut child_update = ResolverUpdate::default();
            child_update.attributes = update
                .attributes
                .add(Locality(Arc::from(target_name.as_str())));
            child_update.endpoints = match &mut grouped_endpoints {
                Ok(grouped) => Ok(grouped.remove(target_name).unwrap_or_default()),
                Err(err) => Err(err.clone()),
            };
            child_update.service_config = update.service_config.clone();
            child_update.resolution_note = update.resolution_note.clone();

            ChildUpdate {
                child_identifier: target_name.clone(),
                child_policy_builder: target_config.child_policy.builder.clone(),
                child_update: Some((child_update, &target_config.child_policy.config)),
            }
        });

        let result = self.child_manager.update(child_updates, channel_controller);

        self.weights = config
            .targets
            .iter()
            .map(|(name, cfg)| (name.clone(), cfg.weight))
            .collect();

        // Always publish on resolver updates/weight changes, so reset the flag
        // by calling child_updated().
        let _ = self.child_manager.child_updated();

        self.update_picker(channel_controller);
        result.map_err(|e| format!("{POLICY_NAME}: failed to update children: {e}"))
    }

    fn work(&mut self, data: Option<WorkData>, channel_controller: &mut dyn ChannelController) {
        self.child_manager.work(data, channel_controller);
        if self.child_manager.child_updated() {
            self.update_picker(channel_controller);
        }
    }

    fn exit_idle(&mut self, channel_controller: &mut dyn ChannelController) {
        self.child_manager.exit_idle(channel_controller);
        if self.child_manager.child_updated() {
            self.update_picker(channel_controller);
        }
    }
}

/// Picker that distributes RPCs across child pickers according to cumulative weights.
#[derive(Debug)]
struct WeightedPicker {
    /// (cumulative weight, picker) pairs in ascending order; the last cumulative
    /// weight is the total. A key sampled from `[0, total)` selects the first
    /// picker whose cumulative weight exceeds it.
    pickers: Vec<(u64, Arc<dyn Picker>)>,
    sampler: Arc<dyn RngSampler>,
}

impl WeightedPicker {
    #[cfg(test)]
    fn new(pickers: Vec<(u64, Arc<dyn Picker>)>) -> Self {
        Self::new_with_sampler(pickers, Arc::new(DefaultRngSampler))
    }

    fn new_with_sampler(
        pickers: Vec<(u64, Arc<dyn Picker>)>,
        sampler: Arc<dyn RngSampler>,
    ) -> Self {
        Self { pickers, sampler }
    }
}

impl Picker for WeightedPicker {
    fn pick(&self, options: PickOptions<'_>) -> PickResult {
        let total_weight = self.pickers.last().map_or(0, |(w, _)| *w);
        if total_weight == 0 {
            debug_assert!(
                false,
                "WeightedPicker constructed with empty pickers or zero total_weight"
            );
            return PickResult::Fail(StatusError::new(
                StatusCodeError::Internal,
                format!("{POLICY_NAME}: picker has no weighted children"),
            ));
        }

        let key = self.sampler.sample(total_weight);
        let index = self.pickers.partition_point(|(w, _)| *w <= key);
        self.pickers[index].1.pick(options)
    }
}

/// Register weighted target as an LbPolicy.
pub(crate) fn reg() {
    START.call_once(|| {
        GLOBAL_LB_REGISTRY.add_builder(WeightedTargetBuilder::default());
    });
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use grpc::__unstable::client::load_balancing::WorkScheduler;
    use grpc::__unstable::client::load_balancing::endpoint_filtering::set_path_in_endpoint;
    use grpc::__unstable::client::load_balancing::round_robin::POLICY_NAME as RR_POLICY_NAME;
    use grpc::__unstable::client::name_resolution::Endpoint;
    use grpc::call_attributes::CallAttributes;
    use grpc::client::RequestHeaders;
    use grpc::core::Address;

    use super::*;

    #[test]
    fn parse_config() {
        let builder = WeightedTargetBuilder::default();

        let valid_json = serde_json::json!({
            "targets": {
                "t1": {
                    "weight": 10,
                    "childPolicy": [{ RR_POLICY_NAME: {} }]
                },
                "t2": {
                    "weight": 90,
                    "childPolicy": [{ RR_POLICY_NAME: {} }]
                }
            }
        });
        let parsed = builder
            .parse_config(&LbConfigJson::new(&valid_json.to_string()).unwrap())
            .expect("a valid config should parse");
        assert_eq!(parsed.targets.len(), 2);
        assert_eq!(parsed.targets.get("t1").unwrap().weight, 10);
        assert_eq!(parsed.targets.get("t2").unwrap().weight, 90);

        let missing_targets = serde_json::json!({});
        let err = builder
            .parse_config(&LbConfigJson::new(&missing_targets.to_string()).unwrap())
            .unwrap_err();
        assert!(err.contains("targets"));

        let empty_targets = serde_json::json!({ "targets": {} });
        let err = builder
            .parse_config(&LbConfigJson::new(&empty_targets.to_string()).unwrap())
            .unwrap_err();
        assert!(err.contains("targets"));

        let zero_weight = serde_json::json!({
            "targets": {
                "t1": {
                    "weight": 0,
                    "childPolicy": [{ RR_POLICY_NAME: {} }]
                }
            }
        });
        let err = builder
            .parse_config(&LbConfigJson::new(&zero_weight.to_string()).unwrap())
            .unwrap_err();
        assert!(err.contains("weight"));
    }

    #[test]
    fn weighted_picker_partitions_weights_correctly() {
        let pickers: Vec<(u64, Arc<dyn Picker>)> = vec![
            (20, tagged_picker("target_a")),
            (50, tagged_picker("target_b")),
            (100, tagged_picker("target_c")),
        ];

        let sampler = Arc::new(MockSampler::new(vec![0, 19, 20, 49, 50, 99]));
        let picker = WeightedPicker::new_with_sampler(pickers, sampler);

        assert_picks_tag(&picker, "target_a");
        assert_picks_tag(&picker, "target_a");
        assert_picks_tag(&picker, "target_b");
        assert_picks_tag(&picker, "target_b");
        assert_picks_tag(&picker, "target_c");
        assert_picks_tag(&picker, "target_c");

        let default_picker = WeightedPicker::new(vec![(50, tagged_picker("target_default"))]);
        assert_picks_tag(&default_picker, "target_default");
    }

    #[test]
    #[cfg_attr(
        debug_assertions,
        should_panic(
            expected = "WeightedPicker constructed with empty pickers or zero total_weight"
        )
    )]
    fn empty_weighted_picker_is_internal_error() {
        let picker = WeightedPicker::new(Vec::new());
        let headers = RequestHeaders::new();
        let mut attrs = CallAttributes::new();
        match picker.pick(PickOptions::new(&headers, &mut attrs)) {
            PickResult::Fail(err) => assert_eq!(err.code(), StatusCodeError::Internal),
            other => panic!("expected Fail(Internal), got {other:?}"),
        }
    }

    #[test]
    fn resolver_update_splits_endpoints_and_injects_locality() {
        let (mut policy, _scheduler, mut controller) = setup_test_policy(vec![15, 45]);

        let config_json = serde_json::json!({
            "targets": {
                "locality_a": {
                    "weight": 30,
                    "childPolicy": [{
                        MOCK_CHILD_POLICY_NAME: {
                            "tag": "child_a",
                            "initial_state": "ready"
                        }
                    }]
                },
                "locality_b": {
                    "weight": 70,
                    "childPolicy": [{
                        MOCK_CHILD_POLICY_NAME: {
                            "tag": "child_b",
                            "initial_state": "ready"
                        }
                    }]
                }
            }
        });
        let cfg = WeightedTargetBuilder::default()
            .parse_config(&LbConfigJson::new(&config_json.to_string()).unwrap())
            .unwrap();

        let mut update = ResolverUpdate::default();
        update.endpoints = Ok(vec![
            make_endpoint("127.0.0.1:9001", "locality_a"),
            make_endpoint("127.0.0.1:9002", "locality_b"),
        ]);
        policy
            .resolver_update(update, &cfg, &mut controller)
            .unwrap();

        let latest = controller.states.last().expect("state update");
        assert_eq!(latest.connectivity_state, ConnectivityState::Ready);

        let child_a = pick(&*latest.picker);
        assert_eq!(child_a.tag, "child_a");
        assert_eq!(child_a.locality.as_deref(), Some("locality_a"));
        assert_eq!(child_a.endpoints.len(), 1);
        assert_eq!(
            &*child_a.endpoints[0].addresses[0].address,
            "127.0.0.1:9001"
        );

        let child_b = pick(&*latest.picker);
        assert_eq!(child_b.tag, "child_b");
        assert_eq!(child_b.locality.as_deref(), Some("locality_b"));
        assert_eq!(child_b.endpoints.len(), 1);
        assert_eq!(
            &*child_b.endpoints[0].addresses[0].address,
            "127.0.0.1:9002"
        );
    }

    #[test]
    fn child_error_still_publishes_picker() {
        let (mut policy, _scheduler, mut controller) = setup_test_policy(vec![0]);

        let config_json = serde_json::json!({
            "targets": {
                "locality_a": {
                    "weight": 30,
                    "childPolicy": [{
                        MOCK_CHILD_POLICY_NAME: {
                            "tag": "child_a",
                            "initial_state": "ready"
                        }
                    }]
                },
                "locality_b": {
                    "weight": 70,
                    "childPolicy": [{
                        MOCK_CHILD_POLICY_NAME: {
                            "tag": "child_b",
                            "initial_state": "ready",
                            "fail_update": true
                        }
                    }]
                }
            }
        });
        let cfg = WeightedTargetBuilder::default()
            .parse_config(&LbConfigJson::new(&config_json.to_string()).unwrap())
            .unwrap();

        let err = policy
            .resolver_update(ResolverUpdate::default(), &cfg, &mut controller)
            .unwrap_err();
        assert_eq!(
            err,
            "weighted_target_experimental: failed to update children: child_b failed"
        );

        let latest = controller.states.last().expect("state update");
        assert_eq!(latest.connectivity_state, ConnectivityState::Ready);
        assert_picks_tag(&*latest.picker, "child_a");
    }

    #[test]
    fn removed_target_is_shut_down() {
        let (mut policy, _scheduler, mut controller) = setup_test_policy(vec![15, 45, 0]);

        let state = apply_json_config(
            &mut policy,
            &mut controller,
            serde_json::json!({
                "targets": {
                    "locality_a": {
                        "weight": 30,
                        "childPolicy": [{
                            MOCK_CHILD_POLICY_NAME: {
                                "tag": "child_a",
                                "initial_state": "ready"
                            }
                        }]
                    },
                    "locality_b": {
                        "weight": 70,
                        "childPolicy": [{
                            MOCK_CHILD_POLICY_NAME: {
                                "tag": "child_b",
                                "initial_state": "ready"
                            }
                        }]
                    }
                }
            }),
        );
        assert_eq!(state.connectivity_state, ConnectivityState::Ready);
        assert_picks_tag(&*state.picker, "child_a");
        assert_picks_tag(&*state.picker, "child_b");

        // Drop locality_b from the config.
        let state = apply_json_config(
            &mut policy,
            &mut controller,
            serde_json::json!({
                "targets": {
                    "locality_a": {
                        "weight": 30,
                        "childPolicy": [{
                            MOCK_CHILD_POLICY_NAME: {
                                "tag": "child_a",
                                "initial_state": "ready"
                            }
                        }]
                    }
                }
            }),
        );
        assert_eq!(state.connectivity_state, ConnectivityState::Ready);
        assert_picks_tag(&*state.picker, "child_a");
    }

    #[test]
    fn state_aggregation_and_picker_selection() {
        let (mut policy, _scheduler, mut controller) = setup_test_policy(vec![0, 10, 80]);

        // 1 Ready + 1 Connecting -> overall Ready, only Ready child is picked.
        let state = apply_json_config(
            &mut policy,
            &mut controller,
            serde_json::json!({
                "targets": {
                    "locality_a": {
                        "weight": 30,
                        "childPolicy": [{
                            MOCK_CHILD_POLICY_NAME: {
                                "tag": "child_a",
                                "initial_state": "ready"
                            }
                        }]
                    },
                    "locality_b": {
                        "weight": 70,
                        "childPolicy": [{
                            MOCK_CHILD_POLICY_NAME: {
                                "tag": "child_b",
                                "initial_state": "connecting"
                            }
                        }]
                    }
                }
            }),
        );
        assert_eq!(state.connectivity_state, ConnectivityState::Ready);
        assert_picks_tag(&*state.picker, "child_a");

        // 1 Connecting + 1 TransientFailure -> overall Connecting (QueuingPicker).
        let state = apply_json_config(
            &mut policy,
            &mut controller,
            serde_json::json!({
                "targets": {
                    "locality_a": {
                        "weight": 30,
                        "childPolicy": [{
                            MOCK_CHILD_POLICY_NAME: {
                                "tag": "child_a",
                                "initial_state": "transient_failure"
                            }
                        }]
                    },
                    "locality_b": {
                        "weight": 70,
                        "childPolicy": [{
                            MOCK_CHILD_POLICY_NAME: {
                                "tag": "child_b",
                                "initial_state": "connecting"
                            }
                        }]
                    }
                }
            }),
        );
        assert_eq!(state.connectivity_state, ConnectivityState::Connecting);
        let headers = RequestHeaders::new();
        let mut attrs = CallAttributes::new();
        assert!(matches!(
            state.picker.pick(PickOptions::new(&headers, &mut attrs)),
            PickResult::Queue
        ));
        assert!(attrs.get::<PickedChild>().is_none());

        // Both TransientFailure on resolver error -> overall TransientFailure, weighted across TF children.
        let cfg = WeightedTargetBuilder::default()
            .parse_config(
                &LbConfigJson::new(
                    &serde_json::json!({
                        "targets": {
                            "locality_a": {
                                "weight": 30,
                                "childPolicy": [{
                                    MOCK_CHILD_POLICY_NAME: {
                                        "tag": "child_a",
                                        "initial_state": "transient_failure"
                                    }
                                }]
                            },
                            "locality_b": {
                                "weight": 70,
                                "childPolicy": [{
                                    MOCK_CHILD_POLICY_NAME: {
                                        "tag": "child_b",
                                        "initial_state": "transient_failure"
                                    }
                                }]
                            }
                        }
                    })
                    .to_string(),
                )
                .unwrap(),
            )
            .unwrap();
        let mut err_update = ResolverUpdate::default();
        err_update.endpoints = Err("resolver error".to_string());
        policy
            .resolver_update(err_update, &cfg, &mut controller)
            .unwrap();

        let latest = controller.states.last().expect("state update");
        assert_eq!(
            latest.connectivity_state,
            ConnectivityState::TransientFailure
        );
        assert_picks_tag(&*latest.picker, "child_a");
        assert_picks_tag(&*latest.picker, "child_b");
    }

    #[test]
    fn work_and_exit_idle_delegate_to_children_and_refresh_picker() {
        let (mut policy, scheduler, mut controller) = setup_test_policy(vec![0]);

        let initial = apply_json_config(
            &mut policy,
            &mut controller,
            serde_json::json!({
                "targets": {
                    "locality_a": {
                        "weight": 100,
                        "childPolicy": [{
                            MOCK_CHILD_POLICY_NAME: {
                                "tag": "child_a",
                                "initial_state": "idle"
                            }
                        }]
                    }
                }
            }),
        );
        assert_eq!(initial.connectivity_state, ConnectivityState::Idle);

        policy.exit_idle(&mut controller);
        let scheduled_work: Vec<_> = scheduler.items.lock().unwrap().drain(..).collect();
        for item in scheduled_work {
            policy.work(item, &mut controller);
        }

        let latest = controller.states.last().expect("state update");
        assert_eq!(latest.connectivity_state, ConnectivityState::Ready);
        assert_picks_tag(&*latest.picker, "child_a");
    }

    #[test]
    fn work_and_exit_idle_without_child_update_publish_nothing() {
        let (mut policy, scheduler, mut controller) = setup_test_policy(vec![]);
        apply_json_config(
            &mut policy,
            &mut controller,
            serde_json::json!({
                "targets": {
                    "locality_a": {
                        "weight": 100,
                        "childPolicy": [{
                            MOCK_CHILD_POLICY_NAME: {
                                "tag": "child_a",
                                "initial_state": "ready"
                            }
                        }]
                    }
                }
            }),
        );

        policy.exit_idle(&mut controller);
        let scheduled_work: Vec<_> = scheduler.items.lock().unwrap().drain(..).collect();
        assert!(!scheduled_work.is_empty());
        for item in scheduled_work {
            policy.work(item, &mut controller);
        }

        assert_eq!(controller.states.len(), 1);
    }

    fn setup_test_policy(
        sample_sequence: Vec<u64>,
    ) -> (
        WeightedTargetPolicy,
        Arc<RecordingWorkScheduler>,
        RecordingChannelController,
    ) {
        GLOBAL_LB_REGISTRY.add_builder(MockChildBuilder);

        let sampler = Arc::new(MockSampler::new(sample_sequence));
        let builder = WeightedTargetBuilder::with_sampler(sampler);
        let scheduler = Arc::new(RecordingWorkScheduler::default());
        let policy = builder.build(LbPolicyOptions {
            work_scheduler: scheduler.clone(),
            runtime: grpc::__unstable::rt::default_runtime(),
        });
        let controller = RecordingChannelController::default();
        (policy, scheduler, controller)
    }

    fn apply_json_config(
        policy: &mut WeightedTargetPolicy,
        controller: &mut RecordingChannelController,
        json: serde_json::Value,
    ) -> LbState {
        let cfg = WeightedTargetBuilder::default()
            .parse_config(&LbConfigJson::new(&json.to_string()).unwrap())
            .unwrap();
        policy
            .resolver_update(ResolverUpdate::default(), &cfg, controller)
            .unwrap();
        controller.states.last().expect("state update").clone()
    }

    fn assert_picks_tag(picker: &dyn Picker, expected: &str) {
        assert_eq!(pick(picker).tag, expected);
    }

    fn pick(picker: &dyn Picker) -> PickedChild {
        let headers = RequestHeaders::new();
        let mut attrs = CallAttributes::new();
        assert!(matches!(
            picker.pick(PickOptions::new(&headers, &mut attrs)),
            PickResult::Queue
        ));
        attrs
            .get::<PickedChild>()
            .expect("picked child attribute")
            .clone()
    }

    #[derive(Debug)]
    struct MockSampler {
        sequences: Mutex<Vec<u64>>,
    }

    impl MockSampler {
        fn new(seq: Vec<u64>) -> Self {
            Self {
                sequences: Mutex::new(seq),
            }
        }
    }

    impl RngSampler for MockSampler {
        fn sample(&self, _total_weight: u64) -> u64 {
            let mut seq = self.sequences.lock().unwrap();
            if seq.is_empty() { 0 } else { seq.remove(0) }
        }
    }

    const MOCK_CHILD_POLICY_NAME: &str = "test_weighted_target_mock_child";

    #[derive(Clone, Debug, Deserialize)]
    struct MockChildConfig {
        tag: String,
        initial_state: String,
        #[serde(default)]
        fail_update: bool,
    }

    /// What a mock child received in its last resolver update. Its picker
    /// writes this into the call attributes.
    #[derive(Clone, Debug, Default)]
    struct PickedChild {
        tag: String,
        locality: Option<Arc<str>>,
        endpoints: Vec<Endpoint>,
    }

    #[derive(Debug)]
    struct TaggedPicker(PickedChild);

    impl Picker for TaggedPicker {
        fn pick(&self, options: PickOptions<'_>) -> PickResult {
            options.call_attributes.add(self.0.clone());
            PickResult::Queue
        }
    }

    fn tagged_picker(tag: &str) -> Arc<dyn Picker> {
        Arc::new(TaggedPicker(PickedChild {
            tag: tag.to_string(),
            ..Default::default()
        }))
    }

    #[derive(Clone, Debug)]
    struct MockChildBuilder;

    #[derive(Debug)]
    struct MockChildPolicy {
        work_scheduler: Arc<dyn WorkScheduler>,
        state: ConnectivityState,
        picked: PickedChild,
    }

    impl MockChildPolicy {
        fn state_for(name: &str) -> ConnectivityState {
            match name {
                "ready" => ConnectivityState::Ready,
                "transient_failure" => ConnectivityState::TransientFailure,
                "idle" => ConnectivityState::Idle,
                _ => ConnectivityState::Connecting,
            }
        }

        fn publish(&self, ctrl: &mut dyn ChannelController) {
            let picker: Arc<dyn Picker> = match self.state {
                ConnectivityState::Ready | ConnectivityState::TransientFailure => {
                    Arc::new(TaggedPicker(self.picked.clone()))
                }
                ConnectivityState::Connecting | ConnectivityState::Idle => Arc::new(QueuingPicker),
            };
            ctrl.update_picker(LbState {
                connectivity_state: self.state,
                picker,
            });
        }
    }

    impl LbPolicyBuilder for MockChildBuilder {
        type LbPolicy = MockChildPolicy;

        fn build(&self, options: LbPolicyOptions) -> Self::LbPolicy {
            MockChildPolicy {
                work_scheduler: options.work_scheduler,
                state: ConnectivityState::Connecting,
                picked: PickedChild::default(),
            }
        }

        fn name(&self) -> &'static str {
            MOCK_CHILD_POLICY_NAME
        }

        fn parse_config(&self, config: &LbConfigJson) -> Result<MockChildConfig, String> {
            config.convert_to().map_err(|e| e.to_string())
        }
    }

    impl LbPolicy for MockChildPolicy {
        type LbConfig = MockChildConfig;

        fn resolver_update(
            &mut self,
            update: ResolverUpdate,
            config: &Self::LbConfig,
            channel_controller: &mut dyn ChannelController,
        ) -> Result<(), String> {
            self.picked = PickedChild {
                tag: config.tag.clone(),
                locality: update.attributes.get::<Locality>().map(|l| l.0.clone()),
                endpoints: update.endpoints.unwrap_or_default(),
            };
            if config.fail_update {
                return Err(format!("{} failed", config.tag));
            }
            self.state = Self::state_for(&config.initial_state);
            self.publish(channel_controller);
            Ok(())
        }

        // An Idle child becomes Ready on the work scheduled by `exit_idle`.
        fn work(
            &mut self,
            _data: Option<WorkData>,
            channel_controller: &mut dyn ChannelController,
        ) {
            if self.state == ConnectivityState::Idle {
                self.state = ConnectivityState::Ready;
                self.publish(channel_controller);
            }
        }

        fn exit_idle(&mut self, _channel_controller: &mut dyn ChannelController) {
            self.work_scheduler.schedule_work(None);
        }
    }

    #[derive(Debug, Default)]
    struct RecordingWorkScheduler {
        items: Mutex<Vec<Option<WorkData>>>,
    }

    impl WorkScheduler for RecordingWorkScheduler {
        fn schedule_work(&self, data: Option<WorkData>) {
            self.items.lock().unwrap().push(data);
        }
    }

    #[derive(Default)]
    struct RecordingChannelController {
        states: Vec<LbState>,
    }

    impl ChannelController for RecordingChannelController {
        fn new_subchannel(
            &mut self,
            _address: &Address,
            _work_scheduler: Arc<dyn WorkScheduler>,
        ) -> (
            Arc<dyn grpc::__unstable::client::load_balancing::subchannel::Subchannel>,
            grpc::__unstable::client::load_balancing::subchannel::SubchannelState,
        ) {
            unimplemented!()
        }

        fn update_picker(&mut self, update: LbState) {
            self.states.push(update);
        }

        fn request_resolution(&mut self) {}
    }

    fn make_endpoint(addr_str: &str, locality_path: &str) -> Endpoint {
        let mut addr = Address::default();
        addr.network_type = "tcp";
        addr.address = addr_str.to_string().into();
        let mut ep = Endpoint::default();
        ep.addresses = vec![addr];
        set_path_in_endpoint(ep, vec![locality_path.to_string()])
    }
}
