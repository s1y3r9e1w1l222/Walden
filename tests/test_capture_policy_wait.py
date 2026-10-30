from __future__ import annotations
import tempfile,time,unittest
from datetime import datetime, timezone
from pathlib import Path
from wechat_local import credential_capture as capture

class PolicyEventMatcherTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.bundle=Path(self.tmp.name)/'Synthetic.app';self.bundle.mkdir()
        self.started=time.time()-1;self.match=capture._PolicyEventMatcher(self.bundle, self.started)
    def tearDown(self):self.tmp.cleanup()
    @staticmethod
    def line(message, seconds=None, pid=123, thread="abc"):
        stamp=datetime.fromtimestamp(time.time() if seconds is None else seconds,timezone.utc).strftime('%Y-%m-%d %H:%M:%S.%f%z')
        return f'{stamp} I  syspolicyd[{pid}:{thread}] [com.apple.syspolicy.exec:default] {message}'
    def batch(self, bundle=None, token='a12b', code='0, 0, 0', seconds=None):
        url=(bundle or self.bundle).as_uri()+'/'
        return [self.line('Skipping TCC check due to process: 2147483000, 0, 2147483000',seconds),
                self.line(f'queueing up scan for code: PST: (path: {token}), (team: (null))',seconds),
                self.line(f'GK Xprotect results: PST: (path: {token}), (team: (null)), XPScan: 0,99,now,(null),(null),{url}',seconds),
                self.line(f'GK scan complete: PST: (path: {token}), (team: (null)), {code}',seconds),
                self.line(f'scan finished, waking up any waiters: PST: (path: {token}), (team: (null))',seconds)]
    def test_tmp_alias_matches_only_same_existing_bundle(self):
        with tempfile.TemporaryDirectory(dir='/private/tmp') as directory:
            real=Path(directory)/'Synthetic.app';real.mkdir()
            alias=Path('/tmp')/Path(directory).name/'Synthetic.app'
            self.assertTrue(alias.samefile(real))
            match=capture._PolicyEventMatcher(real,self.started)
            for line in self.batch(bundle=alias):match.feed(line)
            match.bind_target(2147483000)
            self.assertEqual(match.state,'completed')
            other=Path(directory)/'Other.app';other.mkdir()
            other_alias=Path('/tmp')/Path(directory).name/'Other.app'
            match=capture._PolicyEventMatcher(real,self.started)
            for line in self.batch(bundle=other_alias):match.feed(line)
            match.bind_target(2147483000)
            self.assertEqual(match.state,'unavailable')

    def test_complete_only_for_exact_bundle_and_same_pst(self):
        for line in self.batch():self.match.feed(line)
        self.match.bind_target(2147483000)
        self.assertEqual(self.match.state,'completed')
    def test_other_copy_or_old_event_cannot_complete(self):
        other=Path(self.tmp.name)/'Other.app';other.mkdir()
        for line in self.batch(bundle=other):self.match.feed(line)
        self.match.bind_target(2147483000)
        self.assertNotEqual(self.match.state,'completed')
        for line in self.batch(seconds=self.started-10,token='abcd'):self.match.feed(line)
        self.assertNotEqual(self.match.state,'completed')
    def test_denied_redacted_conflicting_or_nonzero_stop(self):
        lines=self.batch();[self.match.feed(line) for line in lines[:3]];self.match.feed(self.line('policy denied: PST: (path: a12b), (team: (null))'))
        self.assertEqual(self.match.state,'rejected')
        match=capture._PolicyEventMatcher(self.bundle,self.started)
        match.feed(self.line('GK Xprotect results: PST: (path: cdef), XPScan: 0,99,now,(null),(null),<private>'))
        self.assertEqual(match.state,'unavailable')
        match=capture._PolicyEventMatcher(self.bundle,self.started)
        for line in self.batch(code='1, 0, 0'):match.feed(line)
        self.assertNotEqual(match.state,'completed')
    def test_scan_worker_thread_can_differ_from_bound_queue_thread(self):
        lines=self.batch()
        self.match.feed(lines[0]);self.match.feed(lines[1])
        for line in lines[2:]:self.match.feed(line.replace(':abc]', ':def]'))
        self.match.bind_target(2147483000)
        self.assertEqual(self.match.state, 'completed')

    def test_late_old_scan_from_other_pid_is_not_this_attempt(self):
        lines=self.batch()
        lines[0]=self.line('Skipping TCC check due to process: 1234, 0, 1234')
        for line in lines:self.match.feed(line)
        self.match.bind_target(2147483000)
        self.assertEqual(self.match.state,'unavailable')
    def test_reused_pst_or_changed_daemon_is_not_accepted(self):
        lines=self.batch()
        for line in lines[:2]:self.match.feed(line)
        self.match.feed(lines[1])
        self.assertEqual(self.match.state,'unavailable')
        match=capture._PolicyEventMatcher(self.bundle,self.started)
        match.feed(lines[0]);match.feed(self.line('queueing up scan for code: PST: (path: a12b)',pid=456))
        self.assertEqual(match.state,'unavailable')
    def test_early_completion_is_held_until_pid_bound(self):
        match=capture._PolicyEventMatcher(self.bundle,self.started)
        for line in self.batch():match.feed(line)
        self.assertEqual(match.state,'watching')
        match.bind_target(2147483000)
        self.assertEqual(match.state,'completed')

    def test_missing_target_queue_stops_before_wait_even_with_no_deadline(self):
        self.match.bind_target(2147483000)
        self.assertEqual(self.match.state, 'unavailable')

    def test_denial_levels_cannot_be_ignored_before_later_success(self):
        for level in ('Df','E','F'):
            with self.subTest(level=level):
                match=capture._PolicyEventMatcher(self.bundle,self.started)
                lines=self.batch(token='a12b')
                for line in lines[:2]:match.feed(line)
                denial=self.line('policy denied: PST: (path: a12b), (team: (null))').replace(' I  syspolicyd[',f' {level}  syspolicyd[')
                match.feed(denial)
                for line in self.batch(token='cdef'):match.feed(line)
                match.bind_target(2147483000)
                self.assertIn(match.state,{'rejected','unavailable'})

    def test_unrecognized_denial_format_is_unavailable(self):
        lines=self.batch()
        for line in lines[:2]:self.match.feed(line)
        self.match.bind_target(2147483000)
        self.match.feed('syspolicyd unsupported format policy denied: PST: (path: a12b)')
        self.assertEqual(self.match.state,'unavailable')

    def test_bound_queue_explicit_denial_stops_without_waiting_for_url(self):
        lines=self.batch()
        for line in lines[:2]:self.match.feed(line)
        self.match.bind_target(2147483000)
        self.match.feed(self.line('policy denied: PST: (path: a12b), (team: (null))'))
        self.assertEqual(self.match.state, 'rejected')

    def test_bound_queue_nonzero_completion_stops_without_waiting_for_url(self):
        lines=self.batch(code='1, 0, 0')
        for line in lines[:2]:self.match.feed(line)
        self.match.bind_target(2147483000)
        self.match.feed(lines[3])
        self.assertEqual(self.match.state, 'unavailable')

    def test_bound_queue_can_wait_for_late_scan_completion(self):
        lines=self.batch()
        for line in lines[:2]:self.match.feed(line)
        self.match.bind_target(2147483000)
        self.assertEqual(self.match.state, 'watching')
        for line in lines[2:]:self.match.feed(line)
        self.assertEqual(self.match.state, 'completed')

    def test_multiple_psts_cannot_revive_rejection_or_unavailable(self):
        target=2147483000
        for case in ('denied', 'nonzero', 'wrong_pid'):
            for success_first in (False, True):
                with self.subTest(case=case, success_first=success_first):
                    match=capture._PolicyEventMatcher(self.bundle,self.started)
                    good=self.batch(token='cdef')
                    bad=self.batch(token='a12b')
                    if case=='denied':
                        bad=bad[:2]+[self.line('policy denied: PST: (path: a12b), (team: (null))')]
                        expected='rejected'
                    elif case=='nonzero':
                        bad=bad[:2]+[self.line('GK scan complete: PST: (path: a12b), (team: (null)), 1, 0, 0')]
                        expected='unavailable'
                    else:
                        bad[0]=self.line('Skipping TCC check due to process: 2147482999, 0, 2147482999')
                        expected='unavailable'
                    for line in (good+bad if success_first else bad+good):match.feed(line)
                    self.assertEqual(match.state, 'watching')
                    match.bind_target(target)
                    self.assertEqual(match.state, expected)

    def test_terminal_state_stays_closed_after_later_success_events(self):
        for bad in ('denied','nonzero'):
            with self.subTest(bad=bad):
                match=capture._PolicyEventMatcher(self.bundle,self.started)
                lines=self.batch(token='a12b')
                for line in lines[:2]:match.feed(line)
                match.bind_target(2147483000)
                message=('policy denied: PST: (path: a12b), (team: (null))' if bad=='denied' else
                         'GK scan complete: PST: (path: a12b), (team: (null)), 1, 0, 0')
                match.feed(self.line(message));terminal=match.state
                for line in self.batch(token='cdef'):match.feed(line)
                self.assertEqual(match.state,terminal)
                self.assertIn(terminal,{'rejected','unavailable'})

    def test_incomplete_and_different_tokens_do_not_complete(self):
        lines=self.batch();[self.match.feed(line) for line in lines[:3]];self.match.feed(self.batch(token='abcd')[3]);self.match.feed(lines[4])
        self.assertNotEqual(self.match.state,'completed')

if __name__=='__main__':unittest.main()
