package progressbar

import (
	"os"
	"sync"

	"github.com/vbauerster/mpb/v8"
)

// We hope that the printing of logs and progress bars will not conflict.
// So we redirect os.Stdout to mpb.progress.
// ref: https://github.com/vbauerster/mpb/issues/105

var Stdout = newStdoutWriter()
var Progress = sync.OnceValue(func() *mpb.Progress {
	progress := mpb.New(mpb.WithWidth(64))
	Stdout.setProgress(progress)
	return progress
})

type stdoutWriter struct {
	mu sync.RWMutex

	progress *mpb.Progress
}

func newStdoutWriter() *stdoutWriter {
	return &stdoutWriter{}
}

func (w *stdoutWriter) setProgress(progress *mpb.Progress) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.progress = progress
}

func (w *stdoutWriter) Write(p []byte) (n int, err error) {
	w.mu.RLock()
	defer w.mu.RUnlock()

	if w.progress == nil {
		return os.Stdout.Write(p)
	}

	return w.progress.Write(p)
}
