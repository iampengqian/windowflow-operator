# SPDX-License-Identifier: Apache-2.0
"""Runner lifecycle tests using finite iterators and no training dependencies."""

from __future__ import annotations

import dataclasses
import threading
import unittest
from pathlib import Path
from types import SimpleNamespace

from windowflow.client import PlanFailedError, WindowFlowError, WindowHandle, WindowValidationError
from windowflow.runner import WindowContext, WindowEpoch, WindowRunner


class FakeClient:
    def __init__(self, events, *, start_window=0, window_count=3, phase="Running"):
        self.events = events
        self.status = SimpleNamespace(start_window=start_window, window_count=window_count, phase=phase)
        self.held = {}
        self.status_calls = 0
        self.acquire_calls = []
        self.release_calls = []
        self.release_error = None

    def get_status(self):
        self.status_calls += 1
        self.events.append(("status",))
        return self.status

    def acquire(self, index):
        self.acquire_calls.append(index)
        self.events.append(("acquire", index))
        handle = WindowHandle("plan-uid", index, f"window-{index}", f"w{index:06d}",
                              Path(f"/cache/windowflow/plan-uid/w{index:06d}"))
        self.held[index] = handle
        return handle

    def release(self, handle):
        self.release_calls.append(handle.window_index)
        self.events.append(("release", handle.window_index))
        if self.release_error is not None:
            raise self.release_error
        del self.held[handle.window_index]


class RecordingBatches:
    def __init__(self, values, events, tag, *, iter_error=None, next_error=None):
        self.values = iter(values)
        self.events, self.tag = events, tag
        self.iter_error, self.next_error = iter_error, next_error
        self.next_calls = 0

    def __iter__(self):
        self.events.append(("iter", *self.tag))
        if self.iter_error is not None:
            raise self.iter_error
        return self

    def __next__(self):
        self.next_calls += 1
        self.events.append(("next", *self.tag))
        if self.next_error is not None:
            raise self.next_error
        try:
            value = next(self.values)
        except StopIteration:
            self.events.append(("exhausted", *self.tag))
            raise
        self.events.append(("yield", *self.tag, value))
        return value


class EpochFactory:
    def __init__(self, events, values=(10, 20)):
        self.events, self.values = events, values
        self.contexts = []
        self.iterators = []
        self.factory_error = None
        self.iter_error = None
        self.next_error = None
        self.drain_error = None
        self.abort_error = None

    def __call__(self, context):
        self.contexts.append(context)
        tag = (context.handle.window_index, context.epoch)
        self.events.append(("factory", *tag))
        if self.factory_error is not None:
            raise self.factory_error
        batches = RecordingBatches(self.values, self.events, tag,
                                   iter_error=self.iter_error, next_error=self.next_error)
        self.iterators.append(batches)

        def drain():
            self.events.append(("drain", *tag))
            if self.drain_error is not None:
                raise self.drain_error

        def abort():
            self.events.append(("abort", *tag))
            if self.abort_error is not None:
                raise self.abort_error

        return WindowEpoch(batches, drain, abort)


class WindowRunnerTests(unittest.TestCase):
    def fixture(self, *, windows=3, epochs=1, values=(10, 20), start=0, plan_start=0, phase="Running"):
        events = []
        client = FakeClient(events, start_window=plan_start, window_count=windows, phase=phase)
        factory = EpochFactory(events, values)
        runner = WindowRunner(client, factory, epochs_per_window=epochs, start_window=start)
        return runner, client, factory, events

    def assert_failed_and_held(self, runner, client, factory, events):
        self.assertEqual(runner.state, "failed")
        self.assertIn(0, client.held)
        before = list(events)
        with self.assertRaises(WindowFlowError):
            next(runner)
        self.assertEqual(events, before, "A failed runner must not perform more API or dataset work")
        self.assertEqual(client.acquire_calls, [0])

    def test_multiple_windows_and_epochs_exhaust_and_drain_before_release(self):
        runner, client, factory, events = self.fixture(windows=3, epochs=2)
        self.assertEqual(runner.state, "new")
        self.assertIsNone(runner.context)
        self.assertIs(iter(runner), runner)
        self.assertEqual(list(runner), [10, 20] * 6)
        self.assertEqual(runner.state, "completed")
        self.assertIsNone(runner.context)
        self.assertEqual(client.held, {})
        self.assertEqual(client.acquire_calls, [0, 1, 2])
        self.assertEqual(client.release_calls, [0, 1, 2])
        self.assertEqual(client.status_calls, 1)
        self.assertEqual([(c.handle.window_index, c.epoch) for c in factory.contexts],
                         [(w, e) for w in range(3) for e in range(2)])
        for window in range(3):
            for epoch in range(2):
                self.assertLess(events.index(("exhausted", window, epoch)),
                                events.index(("drain", window, epoch)))
                self.assertLess(events.index(("drain", window, epoch)),
                                events.index(("release", window)))
            if window < 2:
                self.assertLess(events.index(("release", window)), events.index(("acquire", window + 1)))
        completed_events = list(events)
        runner.finish()
        runner.close()
        with self.assertRaises(StopIteration):
            next(runner)
        self.assertEqual(events, completed_events)

    def test_run_callback_receives_context_and_finishes_training_before_drain(self):
        runner, client, factory, events = self.fixture(windows=2, epochs=2)

        def train(batch, context):
            self.assertIsInstance(context, WindowContext)
            self.assertIs(context, runner.context)
            self.assertIn(context.handle.window_index, client.held)
            events.append(("train", context.handle.window_index, context.epoch, batch))

        runner.run(train)
        self.assertEqual(runner.state, "completed")
        self.assertEqual(client.status_calls, 1)
        for window in range(2):
            for epoch in range(2):
                self.assertLess(events.index(("train", window, epoch, 20)),
                                events.index(("drain", window, epoch)))
        self.assertFalse(any(e[0] == "abort" for e in events))

    def test_no_per_batch_api_calls(self):
        runner, client, factory, events = self.fixture(windows=1, values=tuple(range(100)))
        self.assertEqual(next(runner), 0)
        baseline = (client.status_calls, list(client.acquire_calls), list(client.release_calls))
        for value in range(1, 100):
            self.assertEqual(next(runner), value)
            self.assertEqual((client.status_calls, client.acquire_calls, client.release_calls), baseline)
        self.assertIn(0, client.held)
        runner.finish()
        self.assertEqual(client.release_calls, [0])

    def test_fixed_step_finish_confirms_exhaustion_and_is_idempotent(self):
        runner, client, factory, events = self.fixture(windows=1)
        self.assertEqual([next(runner), next(runner)], [10, 20])
        self.assertIn(0, client.held)
        self.assertEqual(client.release_calls, [])
        runner.finish()
        self.assertEqual(runner.state, "completed")
        self.assertEqual(client.release_calls, [0])
        self.assertEqual(client.held, {})
        before = list(events)
        runner.finish()
        self.assertEqual(events, before)

    def test_early_finish_buffers_one_batch_and_repeated_finish_does_not_advance(self):
        runner, client, factory, events = self.fixture(windows=1, values=(10, 20, 30))
        self.assertEqual(next(runner), 10)
        with self.assertRaisesRegex(WindowFlowError, "Unconsumed"):
            runner.finish()
        self.assertEqual(runner.state, "active")
        self.assertEqual(factory.iterators[0].next_calls, 2)
        before = list(events)
        with self.assertRaisesRegex(WindowFlowError, "Unconsumed"):
            runner.finish()
        self.assertEqual(events, before)
        self.assertEqual(next(runner), 20)
        self.assertEqual(factory.iterators[0].next_calls, 2)
        self.assertEqual(next(runner), 30)
        self.assertEqual(client.release_calls, [])
        runner.finish()
        self.assertEqual(client.release_calls, [0])
        self.assertEqual(runner.state, "completed")

    def test_finish_rejects_nonfinal_window_or_epoch_without_probing(self):
        for windows, epochs in ((2, 1), (1, 2)):
            with self.subTest(windows=windows, epochs=epochs):
                runner, client, factory, events = self.fixture(windows=windows, epochs=epochs, values=(10,))
                next(runner)
                before = list(events)
                with self.assertRaises(WindowFlowError):
                    runner.finish()
                self.assertEqual(events, before)
                self.assertEqual(runner.state, "active")
                self.assertIn(0, client.held)
                self.assertEqual(client.release_calls, [])

    def test_finish_before_start_or_after_close_does_not_start_work(self):
        runner, client, factory, events = self.fixture(windows=1)
        with self.assertRaises(WindowFlowError):
            runner.finish()
        self.assertEqual(events, [])
        runner.close()
        with self.assertRaises(WindowFlowError):
            runner.finish()
        self.assertEqual(events, [])

    def test_failures_and_keyboard_interrupts_retain_lease_and_stop_iteration(self):
        for location in ("factory", "iter", "next", "drain", "release", "callback"):
            for exception_type in (RuntimeError, KeyboardInterrupt):
                with self.subTest(location=location, exception_type=exception_type.__name__):
                    runner, client, factory, events = self.fixture(values=(10,))
                    error = exception_type(f"{location} failed")
                    if location == "release":
                        client.release_error = error
                    elif location != "callback":
                        setattr(factory, f"{location}_error", error)

                    def train(batch, context):
                        events.append(("train", batch))
                        if location == "callback":
                            raise error

                    with self.assertRaises(exception_type) as raised:
                        runner.run(train)
                    self.assertIs(raised.exception, error)
                    self.assert_failed_and_held(runner, client, factory, events)
                    self.assertEqual(client.release_calls, [0] if location == "release" else [])

    def test_external_iteration_failure_is_terminal_without_automatic_release(self):
        runner, client, factory, events = self.fixture()
        factory.next_error = RuntimeError("decoder")
        with self.assertRaises(RuntimeError):
            next(runner)
        self.assert_failed_and_held(runner, client, factory, events)
        self.assertFalse(any(e[0] in ("drain", "release") for e in events))

    def test_noniterator_stopiteration_is_failure_not_successful_training(self):
        for location in ("factory", "iter", "drain", "release", "callback"):
            with self.subTest(location=location):
                runner, client, factory, events = self.fixture(values=(10,))
                error = StopIteration(f"unexpected {location} StopIteration")
                if location == "release":
                    client.release_error = error
                elif location != "callback":
                    setattr(factory, f"{location}_error", error)

                def train(batch, context):
                    if location == "callback":
                        raise error

                with self.assertRaises(WindowFlowError):
                    runner.run(train)
                self.assert_failed_and_held(runner, client, factory, events)

    def test_finish_failures_and_interrupts_do_not_release_successfully(self):
        for location in ("next", "drain", "release"):
            for exception_type in (RuntimeError, KeyboardInterrupt, StopIteration):
                if location == "next" and exception_type is StopIteration:
                    continue  # Iterator exhaustion is the one legitimate StopIteration.
                with self.subTest(location=location, exception_type=exception_type.__name__):
                    runner, client, factory, events = self.fixture(windows=1, values=(10,))
                    next(runner)
                    error = exception_type("failed while finishing")
                    if location == "next":
                        factory.iterators[0].next_error = error
                    elif location == "drain":
                        factory.drain_error = error
                    else:
                        client.release_error = error
                    expected = WindowFlowError if exception_type is StopIteration else exception_type
                    with self.assertRaises(expected):
                        runner.finish()
                    self.assert_failed_and_held(runner, client, factory, events)

    def test_close_does_not_read_remaining_batches_drain_or_release(self):
        runner, client, factory, events = self.fixture(windows=2, values=(10, 20, 30))
        next(runner)
        runner.close()
        self.assertEqual(runner.state, "stopped")
        self.assertEqual(factory.iterators[0].next_calls, 1)
        self.assertEqual(events.count(("abort", 0, 0)), 1)
        self.assertFalse(any(e[0] in ("drain", "release") for e in events))
        self.assertIn(0, client.held)
        before = list(events)
        runner.close()
        with self.assertRaises(WindowFlowError):
            next(runner)
        self.assertEqual(events, before)

    def test_close_with_buffered_batch_does_not_consume_more(self):
        runner, client, factory, events = self.fixture(windows=1, values=(10, 20, 30))
        next(runner)
        with self.assertRaises(WindowFlowError):
            runner.finish()
        runner.close()
        self.assertEqual(factory.iterators[0].next_calls, 2)
        self.assertIn(0, client.held)
        self.assertEqual(client.release_calls, [])
        self.assertFalse(any(e[0] == "drain" for e in events))

    def test_abort_failure_never_authorizes_release_or_hides_original_training_failure(self):
        for exception_type in (RuntimeError, KeyboardInterrupt):
            with self.subTest(exception_type=exception_type.__name__):
                runner, client, factory, events = self.fixture()
                factory.abort_error = exception_type("cleanup failed")
                original = ValueError("model failed")

                def train(*args):
                    raise original

                with self.assertRaises(WindowFlowError) as raised:
                    runner.run(train)
                self.assertIs(raised.exception.__cause__, original)
                self.assert_failed_and_held(runner, client, factory, events)
                self.assertEqual(client.release_calls, [])

    def test_empty_epoch_fails_instead_of_silently_skipping_data(self):
        runner, client, factory, events = self.fixture(values=())
        with self.assertRaises(WindowValidationError):
            runner.run(lambda *_: self.fail("Empty epoch must not train"))
        self.assert_failed_and_held(runner, client, factory, events)
        self.assertFalse(any(e[0] in ("drain", "release") for e in events))

    def test_invalid_factory_result_retains_acquired_lease(self):
        for result in (None, [], WindowEpoch([], None), WindowEpoch([], lambda: None, abort=3)):
            with self.subTest(result=result):
                runner, client, factory, events = self.fixture()
                runner.epoch_factory = lambda _: result
                with self.assertRaises(WindowValidationError):
                    next(runner)
                self.assert_failed_and_held(runner, client, factory, events)
                self.assertEqual(client.release_calls, [])

    def test_constructor_and_train_callback_validation_has_no_api_side_effects(self):
        events = []
        client, factory = FakeClient(events), EpochFactory(events)
        for kwargs in ({"epochs_per_window": 0}, {"epochs_per_window": -1}, {"epochs_per_window": True},
                       {"epochs_per_window": 1.0}, {"start_window": -1}, {"start_window": True},
                       {"start_window": "0"}):
            with self.subTest(kwargs=kwargs), self.assertRaises(WindowValidationError):
                WindowRunner(client, factory, **kwargs)
        with self.assertRaises(WindowValidationError):
            WindowRunner(client, None)
        runner = WindowRunner(client, factory)
        with self.assertRaises(WindowValidationError):
            runner.run(None)
        self.assertEqual(events, [])

    def test_recovery_start_window_is_explicit_and_skips_earlier_ordinals(self):
        runner, client, factory, events = self.fixture(windows=4, start=2, plan_start=1, values=(10,))
        self.assertEqual(list(runner), [10, 10])
        self.assertEqual(client.acquire_calls, [2, 3])
        self.assertEqual(client.release_calls, [2, 3])
        self.assertEqual([c.epoch for c in factory.contexts], [0, 0])
        self.assertEqual(runner.state, "completed")

    def test_start_before_attempt_or_beyond_schedule_is_rejected(self):
        for start, plan_start in ((0, 1), (4, 0)):
            with self.subTest(start=start, plan_start=plan_start):
                runner, client, factory, events = self.fixture(windows=3, start=start, plan_start=plan_start)
                with self.assertRaises(WindowValidationError):
                    next(runner)
                self.assertEqual(runner.state, "failed")
                self.assertEqual(client.acquire_calls, [])

    def test_start_at_end_is_noop_only_for_nonfailed_plan(self):
        runner, client, factory, events = self.fixture(windows=3, start=3)
        self.assertEqual(list(runner), [])
        self.assertEqual(runner.state, "completed")
        self.assertEqual(client.acquire_calls, [])
        for start in (0, 3):
            with self.subTest(start=start):
                runner, client, factory, events = self.fixture(windows=3, start=start, phase="Failed")
                with self.assertRaises(PlanFailedError):
                    runner.run(lambda *_: self.fail("Failed plan must never train"))
                self.assertEqual(runner.state, "failed")
                self.assertEqual(client.acquire_calls, [])

    def test_context_and_epoch_contract_are_immutable(self):
        runner, client, factory, events = self.fixture()
        next(runner)
        with self.assertRaises(dataclasses.FrozenInstanceError):
            runner.context.epoch = 10
        epoch = WindowEpoch([1], lambda: None)
        with self.assertRaises(dataclasses.FrozenInstanceError):
            epoch.drain = None
        runner.close()

    def test_reentrant_factory_call_is_rejected_without_second_acquire(self):
        runner, client, factory, events = self.fixture()

        def reentrant(context):
            next(runner)
            return WindowEpoch([1], lambda: None)

        runner.epoch_factory = reentrant
        with self.assertRaises(WindowFlowError):
            next(runner)
        self.assert_failed_and_held(runner, client, factory, events)
        self.assertEqual(client.release_calls, [])

    def test_training_callback_cannot_reenter_next_finish_close_or_run(self):
        for action in ("next", "finish", "close", "run"):
            with self.subTest(action=action):
                runner, client, factory, events = self.fixture(windows=1, values=(10,))

                def train(*args):
                    if action == "next":
                        next(runner)
                    elif action == "run":
                        runner.run(lambda *_: None)
                    else:
                        getattr(runner, action)()

                with self.assertRaises(WindowFlowError):
                    runner.run(train)
                self.assert_failed_and_held(runner, client, factory, events)
                self.assertEqual(client.release_calls, [])
                self.assertFalse(any(e[0] == "drain" for e in events))

    def test_cross_thread_next_finish_close_and_run_are_rejected_without_work(self):
        for action in ("next", "finish", "close", "run"):
            with self.subTest(action=action):
                runner, client, factory, events = self.fixture(windows=1, values=(10, 20))
                next(runner)
                before, errors = list(events), []

                def foreign_thread():
                    try:
                        if action == "next":
                            next(runner)
                        elif action == "run":
                            runner.run(lambda *_: None)
                        else:
                            getattr(runner, action)()
                    except BaseException as exc:
                        errors.append(exc)

                thread = threading.Thread(target=foreign_thread, daemon=True)
                thread.start()
                thread.join(timeout=2)
                self.assertFalse(thread.is_alive(), "Cross-thread guard must fail promptly, not block")
                self.assertEqual(len(errors), 1)
                self.assertIsInstance(errors[0], WindowFlowError)
                self.assertEqual(events, before)
                self.assertIn(0, client.held)
                self.assertEqual(client.release_calls, [])
                runner.close()


if __name__ == "__main__":
    unittest.main()
