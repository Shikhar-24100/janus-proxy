package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
)

func TestOutboxHardCrashDuringPartialAppend(t *testing.T) {
	o := outboxFixture(t)
	first, second := sampleUsageEvent(), sampleUsageEvent()
	if err := o.put(first); err != nil {
		t.Fatal(err)
	}
	o.close()
	data, _ := json.Marshal(second)
	cmd := exec.Command(os.Args[0], "-test.run=^TestOutboxCrashHelper$", "-test.timeout=1m")
	cmd.Env = append(os.Environ(), "JANUS_TEST_OUTBOX_CHILD="+o.dir, "JANUS_TEST_OUTBOX_EVENT="+string(data), "JANUS_TEST_OUTBOX_PHASE=partial")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cmd.Process.Kill() })
	scanner := bufio.NewScanner(stdout)
	if !scanner.Scan() || scanner.Text() != "outbox-partial" {
		t.Fatal("child did not reach partial append")
	}
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = cmd.Wait()
	next := reopenOutbox(t, o.dir)
	if len(next.recovered) != 1 || next.recovered[0].RequestID != first.RequestID || next.repaired.Load() != 1 {
		t.Fatal("partial append crash lost confirmed event or retained incomplete record")
	}
}

func reopenOutbox(t *testing.T, path string) *usageOutbox {
	t.Helper()
	o, err := openUsageOutbox(path, "1")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(o.close)
	return o
}

func TestOutboxRepairsOnlyIncompleteTail(t *testing.T) {
	for _, part := range []string{"header", "payload"} {
		t.Run(part, func(t *testing.T) {
			o := outboxFixture(t)
			first, second := sampleUsageEvent(), sampleUsageEvent()
			if err := o.put(first); err != nil {
				t.Fatal(err)
			}
			o.close()
			data, _ := json.Marshal(second)
			frame := encodeOutboxFrame(outboxRecord{Put: data})
			if part == "header" {
				frame = frame[:7]
			} else {
				frame = frame[:len(frame)-5]
			}
			f, err := os.OpenFile(filepath.Join(o.dir, outboxJournalName), os.O_APPEND|os.O_WRONLY, 0600)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := f.Write(frame); err != nil {
				t.Fatal(err)
			}
			f.Close()
			next := reopenOutbox(t, o.dir)
			if len(next.recovered) != 1 || next.recovered[0].RequestID != first.RequestID || next.repaired.Load() != 1 {
				t.Fatal("partial tail damaged confirmed event")
			}
			if err := next.put(second); err != nil {
				t.Fatal(err)
			}
			next.close()
			again := reopenOutbox(t, o.dir)
			if len(again.recovered) != 2 || again.repaired.Load() != 0 {
				t.Fatal("repair did not permit safe subsequent appends")
			}
		})
	}
}

func TestOutboxChecksumCorruptionIsRetained(t *testing.T) {
	o := outboxFixture(t)
	if err := o.put(sampleUsageEvent()); err != nil {
		t.Fatal(err)
	}
	o.close()
	path := filepath.Join(o.dir, outboxJournalName)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	data[len(data)-3] ^= 1
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	if next, err := openUsageOutbox(o.dir, "1"); err == nil {
		next.close()
		t.Fatal("checksum corruption was accepted")
	}
	if info, err := os.Stat(path); err != nil || info.Size() != int64(len(data)) {
		t.Fatal("corrupt journal was discarded or truncated")
	}
}

func TestOutboxPartialWriteRetriesFromConfirmedOffset(t *testing.T) {
	o := outboxFixture(t)
	first, second := sampleUsageEvent(), sampleUsageEvent()
	if err := o.put(first); err != nil {
		t.Fatal(err)
	}
	o.writeFile = func(f *os.File, data []byte) (int, error) {
		n, err := f.Write(data[:len(data)/2])
		if err != nil {
			return n, err
		}
		return n, io.ErrShortWrite
	}
	if err := o.put(second); err == nil {
		t.Fatal("partial write was confirmed")
	}
	if n, _, _, _, blocked := o.snapshot(); n != 1 || !blocked {
		t.Fatal("failed append changed durable index or kept admission open")
	}
	o.writeFile = func(f *os.File, data []byte) (int, error) { return f.Write(data) }
	if err := o.put(second); err != nil {
		t.Fatal(err)
	}
	o.close()
	next := reopenOutbox(t, o.dir)
	if len(next.recovered) != 2 || next.repaired.Load() != 0 {
		t.Fatal("retry left an invalid tail or lost confirmed data")
	}
}

func TestOutboxCompactionPreservesOnlyLiveEvents(t *testing.T) {
	o := outboxFixture(t)
	first, second, third := sampleUsageEvent(), sampleUsageEvent(), sampleUsageEvent()
	if err := o.put(second); err != nil {
		t.Fatal(err)
	}
	for range 3 {
		if err := o.put(first); err != nil {
			t.Fatal(err)
		}
		if err := o.remove(first.RequestID); err != nil {
			t.Fatal(err)
		}
	}
	o.compactAt = 1
	if err := o.put(third); err != nil {
		t.Fatal(err)
	}
	if o.compactions.Load() != 1 {
		t.Fatal("dead history was not compacted")
	}
	o.close()
	next := reopenOutbox(t, o.dir)
	n, _, _, _, _ := next.snapshot()
	if n != 2 || next.records[first.RequestID] != nil || next.records[second.RequestID] == nil || next.records[third.RequestID] == nil {
		t.Fatal("checkpoint lost live events or resurrected acknowledged events")
	}
}

func TestOutboxCompactionDirectoryFailureRetriesSafely(t *testing.T) {
	o := outboxFixture(t)
	live, dead, third := sampleUsageEvent(), sampleUsageEvent(), sampleUsageEvent()
	if err := o.put(live); err != nil {
		t.Fatal(err)
	}
	for range 3 {
		if err := o.put(dead); err != nil {
			t.Fatal(err)
		}
		if err := o.remove(dead.RequestID); err != nil {
			t.Fatal(err)
		}
	}
	o.compactAt = 1
	o.syncDir = func(string) error { return errors.New("directory flush unavailable") }
	if err := o.put(third); err == nil {
		t.Fatal("unconfirmed checkpoint permitted an append")
	}
	if n, _, _, _, blocked := o.snapshot(); n != 1 || !blocked {
		t.Fatal("failed checkpoint changed live records or left admission open")
	}
	o.syncDir = syncOutboxDir
	if err := o.put(third); err != nil {
		t.Fatal(err)
	}
	o.close()
	next := reopenOutbox(t, o.dir)
	if len(next.recovered) != 2 || next.records[live.RequestID] == nil || next.records[third.RequestID] == nil || next.records[dead.RequestID] != nil {
		t.Fatal("checkpoint retry lost or resurrected events")
	}
}
func TestOutboxMigratesLegacyFiles(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("durable outbox requires Linux")
	}
	path := t.TempDir()
	event := sampleUsageEvent()
	data, _ := json.Marshal(event)
	legacyPath := filepath.Join(path, outboxName(event.RequestID))
	if err := os.WriteFile(legacyPath, data, 0600); err != nil {
		t.Fatal(err)
	}
	o := reopenOutbox(t, path)
	if _, err := os.Stat(legacyPath); !os.IsNotExist(err) {
		t.Fatal("migration left old file behind")
	}
	if len(o.recovered) != 1 || o.recovered[0].RequestID != event.RequestID {
		t.Fatal("migration lost request identity")
	}
	if err := o.remove(event.RequestID); err != nil {
		t.Fatal(err)
	}
	o.close()
	next := reopenOutbox(t, path)
	if len(next.recovered) != 0 {
		t.Fatal("journal acknowledgement did not survive restart")
	}
}
