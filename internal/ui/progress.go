package ui

import (
	"fmt"
	"os"
	"sync"
)

// Progress displays a simple progress bar in the terminal.
type Progress struct {
	label   string
	total   int
	current int
	mu      sync.Mutex
	active  bool
}

// NewProgress creates a new Progress instance.
func NewProgress() *Progress {
	return &Progress{}
}

// Start begins a progress display with the given label and total count.
func (p *Progress) Start(label string, total int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.label = label
	p.total = total
	p.current = 0
	p.active = true

	if jsonMode || !isTTY() {
		fmt.Printf("%s: starting (%d total)\n", label, total)
		return
	}
	p.render()
}

// Increment advances the progress bar by one.
func (p *Progress) Increment() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.active {
		return
	}
	p.current++
	if p.current > p.total {
		p.current = p.total
	}

	if jsonMode || !isTTY() {
		return
	}
	p.render()
}

// Done completes the progress bar.
func (p *Progress) Done() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.active {
		return
	}
	p.current = p.total
	p.active = false

	if jsonMode || !isTTY() {
		fmt.Printf("%s: done\n", p.label)
		return
	}
	p.render()
	fmt.Fprintln(os.Stdout)
}

// render draws the progress bar using \r to overwrite the current line.
func (p *Progress) render() {
	const barWidth = 30
	var filled int
	if p.total > 0 {
		filled = (p.current * barWidth) / p.total
	}
	empty := barWidth - filled

	pct := 0
	if p.total > 0 {
		pct = (p.current * 100) / p.total
	}

	bar := make([]byte, barWidth)
	for i := 0; i < filled; i++ {
		bar[i] = '#'
	}
	for i := filled; i < filled+empty; i++ {
		bar[i] = '.'
	}

	fmt.Fprintf(os.Stdout, "\r  %s [%s] %d%% (%d/%d)", p.label, string(bar), pct, p.current, p.total)
}
