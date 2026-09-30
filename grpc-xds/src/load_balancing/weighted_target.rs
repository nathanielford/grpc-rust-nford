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

        // Always publish: weight changes and removed targets alter the picker
        // even if no child reported a new state. `child_updated()` is called
        // only to reset its flag so the next `work()`/`exit_idle()` doesn't
        // republish.
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
    use std::collections::VecDeque;
    use std::sync::Mutex;

    use grpc::__unstable::client::load_balancing::WorkScheduler;
    use grpc::__unstable::client::load_balancing::endpoint_filtering::set_path_in_endpoint;
    use grpc::__unstable::client::load_balancing::round_robin::POLICY_NAME as RR_POLICY_NAME;
    use grpc::__unstable::client::load_balancing::subchannel::Subchannel;
    use grpc::__unstable::client::load_balancing::subchannel::SubchannelState;
    use grpc::__unstable::client::name_resolution::Endpoint;
    use grpc::call_attributes::CallAttributes;
    use grpc::client::RequestHeaders;
    use grpc::core::Address;
    use serde::Serialize;
    use serde_json::json;

    use super::*;

    #[test]
    fn parse_config() {
        let parse = |json: serde_json::Value| {
            WeightedTargetBuilder::default()
                .parse_config(&LbConfigJson::new(&json.to_string()).unwrap())
        };
        let rr = json!([{ RR_POLICY_NAME: {} }]);

        let cfg = parse(json!({ "targets": {
            "t1": { "weight": 10, "childPolicy": rr },
            "t2": { "weight": 90, "childPolicy": rr }
        }}))
        .expect("valid config should parse");
        let weights: Vec<_> = cfg
            .targets
            .iter()
            .map(|(name, target)| (name.as_str(), target.weight))
            .collect();
        assert_eq!(weights, [("t1", 10), ("t2", 90)]);

        let err = |json| parse(json).expect_err("invalid config should fail to parse");
        assert!(err(json!({})).contains("targets"));
        assert_eq!(
            err(json!({ "targets": {} })),
            "weighted_target_experimental: invalid config: 'targets' must be non-empty"
        );
        assert_eq!(
            err(json!({ "targets": { "t1": { "weight": 0, "childPolicy": rr } } })),
            "weighted_target_experimental: invalid config: target 't1': weight must be greater than 0"
        );
    }

    #[test]
    fn weighted_picker_partitions_weights_correctly() {
        let pickers: Vec<(u64, Arc<dyn Picker>)> = vec![
            (20, mock_picker("a")),
            (50, mock_picker("b")),
            (100, mock_picker("c")),
        ];
        let picker = WeightedPicker::new_with_sampler(pickers, sampler(&[0, 19, 20, 49, 50, 99]));
        for tag in ["a", "a", "b", "b", "c", "c"] {
            assert_picks(&picker, tag);
        }

        let default_picker = WeightedPicker::new_with_sampler(
            vec![(50, mock_picker("a"))],
            Arc::new(DefaultRngSampler),
        );
        assert_picks(&default_picker, "a");
    }

    #[test]
    #[cfg_attr(
        debug_assertions,
        should_panic(
            expected = "WeightedPicker constructed with empty pickers or zero total_weight"
        )
    )]
    fn empty_weighted_picker_is_internal_error() {
        let picker = WeightedPicker::new_with_sampler(Vec::new(), sampler(&[]));
        let headers = RequestHeaders::new();
        let mut attrs = CallAttributes::new();
        match picker.pick(PickOptions::new(&headers, &mut attrs)) {
            PickResult::Fail(err) => assert_eq!(err.code(), StatusCodeError::Internal),
            other => panic!("expected Fail(Internal), got {other:?}"),
        }
    }

    #[test]
    fn resolver_update_splits_endpoints_and_injects_locality() {
        let mut t = Harness::new(&[0, 30]);
        let mut update = ResolverUpdate::default();
        update.endpoints = Ok(vec![
            endpoint(&["a", "a1"]),
            endpoint(&["b", "b1"]),
            endpoint(&["b", "b2"]),
            endpoint(&["c", "c1"]),
        ]);
        t.update(update, &[("a", 30, Mock::Ready), ("b", 70, Mock::Ready)])
            .unwrap();

        // Each child gets only the endpoints under its name, with that path
        // element removed. "c" is not a target, so its endpoint goes nowhere.
        let picker = t.take_state().picker;
        let a = assert_picks(&*picker, "a");
        assert_eq!(a.endpoints, Ok(vec![endpoint(&["a1"])]));
        assert_eq!(a.attributes.get::<Locality>(), Some(&Locality("a".into())));
        let b = assert_picks(&*picker, "b");
        assert_eq!(b.endpoints, Ok(vec![endpoint(&["b1"]), endpoint(&["b2"])]));
        assert_eq!(b.attributes.get::<Locality>(), Some(&Locality("b".into())));
    }

    #[test]
    fn resolver_update_forwards_errors_and_note_to_children() {
        let mut t = Harness::new(&[0, 30]);
        let mut update = ResolverUpdate::default();
        update.endpoints = Err("endpoints error".to_string());
        update.service_config = Err("service config error".to_string());
        update.resolution_note = Some("note".to_string());
        t.update(update, &[("a", 30, Mock::Ready), ("b", 70, Mock::Ready)])
            .unwrap();

        let picker = t.take_state().picker;
        for locality in ["a", "b"] {
            let update = assert_picks(&*picker, locality);
            assert_eq!(update.endpoints, Err("endpoints error".to_string()));
            assert_eq!(update.service_config.unwrap_err(), "service config error");
            assert_eq!(update.resolution_note.as_deref(), Some("note"));
        }
    }

    #[test]
    fn child_error_still_publishes_picker() {
        let mut t = Harness::new(&[0]);
        let err = t
            .update(
                ResolverUpdate::default(),
                &[("a", 30, Mock::Ready), ("b", 70, Mock::FailUpdate)],
            )
            .unwrap_err();
        assert_eq!(
            err,
            "weighted_target_experimental: failed to update children: mock failure"
        );

        let state = t.take_state();
        assert_eq!(state.connectivity_state, ConnectivityState::Ready);
        assert_picks(&*state.picker, "a");
    }

    #[test]
    fn config_update_republishes_picker() {
        // Key 30 selects "b" under weights 30/70 and "a" under 80/20.
        let mut t = Harness::new(&[30, 30, 0]);
        let state = t.apply(&[("a", 30, Mock::Ready), ("b", 70, Mock::Ready)]);
        assert_picks(&*state.picker, "b");

        // The children's states are unchanged in the next two updates, so
        // only the new weights and the removal of "a" change the picker.
        let state = t.apply(&[("a", 80, Mock::Ready), ("b", 20, Mock::Ready)]);
        assert_picks(&*state.picker, "a");

        let state = t.apply(&[("b", 20, Mock::Ready)]);
        assert_picks(&*state.picker, "b");
    }

    #[test]
    fn ready_picker_excludes_non_ready_children() {
        // Key 0 would select "a" if non-Ready children were included.
        let mut t = Harness::new(&[0]);
        let state = t.apply(&[
            ("a", 10, Mock::Connecting),
            ("b", 20, Mock::TransientFailure),
            ("c", 70, Mock::Ready),
        ]);
        assert_eq!(state.connectivity_state, ConnectivityState::Ready);
        assert_picks(&*state.picker, "c");
    }

    #[test]
    fn transient_failure_picker_spans_failing_children() {
        let mut t = Harness::new(&[0, 30]);
        let state = t.apply(&[
            ("a", 30, Mock::TransientFailure),
            ("b", 70, Mock::TransientFailure),
        ]);
        assert_eq!(
            state.connectivity_state,
            ConnectivityState::TransientFailure
        );
        assert_picks(&*state.picker, "a");
        assert_picks(&*state.picker, "b");
    }

    #[test]
    fn connecting_and_idle_queue_without_reaching_children() {
        // No keys: a weighted pick would panic in the sampler.
        let mut t = Harness::new(&[]);
        for (behavior, want) in [
            (Mock::Connecting, ConnectivityState::Connecting),
            (Mock::Idle, ConnectivityState::Idle),
        ] {
            let state = t.apply(&[("a", 30, Mock::TransientFailure), ("b", 70, behavior)]);
            assert_eq!(state.connectivity_state, want);
            assert!(pick(&*state.picker).is_none());
        }
    }

    #[test]
    fn exit_idle_and_work_republish_only_on_child_update() {
        let mut t = Harness::new(&[]);
        t.apply(&[("a", 100, Mock::Ready)]);
        t.policy.exit_idle(&mut t.controller);
        t.run_work();
        assert!(
            t.controller.states.is_empty(),
            "policy should not publish when no child updated"
        );

        t.apply(&[("a", 100, Mock::Idle)]);
        t.policy.exit_idle(&mut t.controller);
        assert_eq!(
            t.take_state().connectivity_state,
            ConnectivityState::Connecting
        );
        t.run_work();
        assert_eq!(t.take_state().connectivity_state, ConnectivityState::Ready);
    }

    /// A `WeightedTargetPolicy` whose sampler returns the given keys in order.
    struct Harness {
        policy: WeightedTargetPolicy,
        controller: RecordingChannelController,
        scheduler: Arc<RecordingWorkScheduler>,
    }

    impl Harness {
        fn new(keys: &[u64]) -> Self {
            GLOBAL_LB_REGISTRY.add_builder(MockChildBuilder);
            let scheduler = Arc::new(RecordingWorkScheduler::default());
            let sampler = sampler(keys);
            let policy = WeightedTargetBuilder::with_sampler(sampler).build(LbPolicyOptions {
                work_scheduler: scheduler.clone(),
                runtime: grpc::__unstable::rt::default_runtime(),
            });
            Self {
                policy,
                controller: RecordingChannelController::default(),
                scheduler,
            }
        }

        /// Sends `update` with one mock child per `(name, weight, behavior)`.
        /// Each child is tagged with its target name.
        fn update(
            &mut self,
            update: ResolverUpdate,
            targets: &[(&str, u32, Mock)],
        ) -> Result<(), String> {
            let targets: serde_json::Map<_, _> = targets
                .iter()
                .map(|&(name, weight, behavior)| {
                    let child_config = json!({ "tag": name, "behavior": behavior });
                    let child_policy = json!([{ MOCK_CHILD_POLICY_NAME: child_config }]);
                    let target = json!({ "weight": weight, "childPolicy": child_policy });
                    (name.to_string(), target)
                })
                .collect();
            let json = json!({ "targets": targets }).to_string();
            let config = WeightedTargetBuilder::default()
                .parse_config(&LbConfigJson::new(&json).unwrap())
                .expect("test config should parse");
            self.policy
                .resolver_update(update, &config, &mut self.controller)
        }

        /// Sends `targets` with an empty resolver update and returns the
        /// published state.
        fn apply(&mut self, targets: &[(&str, u32, Mock)]) -> LbState {
            self.update(ResolverUpdate::default(), targets)
                .expect("resolver update should succeed");
            self.take_state()
        }

        /// Returns the one state published since the last call.
        fn take_state(&mut self) -> LbState {
            let mut states = std::mem::take(&mut self.controller.states);
            assert_eq!(states.len(), 1, "policy should publish exactly one state");
            states.pop().unwrap()
        }

        fn run_work(&mut self) {
            let work = self.scheduler.0.lock().unwrap().pop();
            let work = work.expect("a child should have scheduled work");
            self.policy.work(work, &mut self.controller);
        }
    }

    /// Picks once and returns what the child that handled the pick reported,
    /// or `None` if the pick was queued without reaching a child.
    fn pick(picker: &dyn Picker) -> Option<PickedChild> {
        let headers = RequestHeaders::new();
        let mut attrs = CallAttributes::new();
        let result = picker.pick(PickOptions::new(&headers, &mut attrs));
        assert!(
            matches!(result, PickResult::Queue),
            "pick should queue, got {result:?}"
        );
        attrs.get::<PickedChild>().cloned()
    }

    /// Asserts that a pick reaches the child tagged `tag`, and returns the
    /// resolver update that child received.
    fn assert_picks(picker: &dyn Picker, tag: &str) -> ResolverUpdate {
        let picked = pick(picker).expect("pick should reach a child");
        assert_eq!(picked.tag, tag);
        picked.update
    }

    fn endpoint(path: &[&str]) -> Endpoint {
        set_path_in_endpoint(
            Endpoint::default(),
            path.iter().map(|p| p.to_string()).collect(),
        )
    }

    fn sampler(keys: &[u64]) -> Arc<MockSampler> {
        Arc::new(MockSampler(Mutex::new(keys.to_vec().into())))
    }

    /// Returns the given keys in order. Panics when the keys run out or a key
    /// is not below the total weight, so no pick depends on an unplanned key.
    #[derive(Debug)]
    struct MockSampler(Mutex<VecDeque<u64>>);

    impl RngSampler for MockSampler {
        fn sample(&self, total_weight: u64) -> u64 {
            let key = self.0.lock().unwrap().pop_front();
            let key = key.expect("test should provide a key for every weighted pick");
            assert!(
                key < total_weight,
                "key {key} should be below total weight {total_weight}"
            );
            key
        }
    }

    /// What a mock child's picker adds to the call attributes.
    #[derive(Clone, Debug)]
    struct PickedChild {
        tag: String,
        /// The last resolver update the child received.
        update: ResolverUpdate,
    }

    #[derive(Debug)]
    struct MockPicker(PickedChild);

    impl Picker for MockPicker {
        fn pick(&self, options: PickOptions<'_>) -> PickResult {
            options.call_attributes.add(self.0.clone());
            PickResult::Queue
        }
    }

    fn mock_picker(tag: &str) -> Arc<dyn Picker> {
        Arc::new(MockPicker(PickedChild {
            tag: tag.to_string(),
            update: ResolverUpdate::default(),
        }))
    }

    const MOCK_CHILD_POLICY_NAME: &str = "test_weighted_target_mock_child";

    /// How the mock child responds to a resolver update.
    #[derive(Clone, Copy, Debug, Deserialize, Serialize)]
    #[serde(rename_all = "snake_case")]
    enum Mock {
        Idle,
        Connecting,
        Ready,
        TransientFailure,
        /// Returns an error without publishing a state.
        FailUpdate,
    }

    #[derive(Debug, Deserialize)]
    struct MockConfig {
        tag: String,
        behavior: Mock,
    }

    #[derive(Debug)]
    struct MockChildBuilder;

    /// Publishes a `MockPicker` in every state, so a pick that reaches a
    /// child is always observable.
    #[derive(Debug)]
    struct MockChildPolicy {
        work_scheduler: Arc<dyn WorkScheduler>,
        state: Option<ConnectivityState>,
        picked: PickedChild,
    }

    impl MockChildPolicy {
        /// Publishes only on a state change, so an update that leaves the state
        /// unchanged is not reported to the parent as a child update.
        fn set_state(&mut self, state: ConnectivityState, controller: &mut dyn ChannelController) {
            if self.state != Some(state) {
                self.state = Some(state);
                controller.update_picker(LbState {
                    connectivity_state: state,
                    picker: Arc::new(MockPicker(self.picked.clone())),
                });
            }
        }
    }

    impl LbPolicyBuilder for MockChildBuilder {
        type LbPolicy = MockChildPolicy;

        fn build(&self, options: LbPolicyOptions) -> Self::LbPolicy {
            MockChildPolicy {
                work_scheduler: options.work_scheduler,
                state: None,
                picked: PickedChild {
                    tag: String::new(),
                    update: ResolverUpdate::default(),
                },
            }
        }

        fn name(&self) -> &'static str {
            MOCK_CHILD_POLICY_NAME
        }

        fn parse_config(&self, config: &LbConfigJson) -> Result<MockConfig, String> {
            config.convert_to().map_err(|e| e.to_string())
        }
    }

    impl LbPolicy for MockChildPolicy {
        type LbConfig = MockConfig;

        fn resolver_update(
            &mut self,
            update: ResolverUpdate,
            config: &MockConfig,
            channel_controller: &mut dyn ChannelController,
        ) -> Result<(), String> {
            self.picked = PickedChild {
                tag: config.tag.clone(),
                update,
            };
            let state = match config.behavior {
                Mock::Idle => ConnectivityState::Idle,
                Mock::Connecting => ConnectivityState::Connecting,
                Mock::Ready => ConnectivityState::Ready,
                Mock::TransientFailure => ConnectivityState::TransientFailure,
                Mock::FailUpdate => return Err("mock failure".to_string()),
            };
            self.set_state(state, channel_controller);
            Ok(())
        }

        // Idle -> Connecting on `exit_idle`, then Connecting -> Ready on the
        // work that `exit_idle` schedules.
        fn exit_idle(&mut self, channel_controller: &mut dyn ChannelController) {
            self.work_scheduler.schedule_work(None);
            if self.state == Some(ConnectivityState::Idle) {
                self.set_state(ConnectivityState::Connecting, channel_controller);
            }
        }

        fn work(
            &mut self,
            _data: Option<WorkData>,
            channel_controller: &mut dyn ChannelController,
        ) {
            if self.state == Some(ConnectivityState::Connecting) {
                self.set_state(ConnectivityState::Ready, channel_controller);
            }
        }
    }

    #[derive(Debug, Default)]
    struct RecordingWorkScheduler(Mutex<Vec<Option<WorkData>>>);

    impl WorkScheduler for RecordingWorkScheduler {
        fn schedule_work(&self, data: Option<WorkData>) {
            self.0.lock().unwrap().push(data);
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
        ) -> (Arc<dyn Subchannel>, SubchannelState) {
            unimplemented!()
        }

        fn update_picker(&mut self, update: LbState) {
            self.states.push(update);
        }

        fn request_resolution(&mut self) {
            unimplemented!()
        }
    }
}
