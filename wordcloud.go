package wordclouds

import (
	"fmt"
	"image"
	"image/color"
	"iter"
	"math"
	"math/rand"
	"runtime"
	"sync"
	"time"

	"github.com/fogleman/gg"
	"github.com/stergiotis/boxer/public/containers/co"
	"github.com/stergiotis/boxer/public/math32"
	"golang.org/x/exp/slices"
	"golang.org/x/image/font"
)

type FontDrawingContextI interface {
	SetFontFace(face font.Face)
}

type VectorDrawingContextI interface {
	SetColor(col color.Color)
	MeasureString(s string) (w, h float64)
	// DrawStringAnchored draws the specified text at the specified anchor point.
	// The anchor point is x - w * ax, y - h * ay, where w, h is the size of the
	// text. Use ax=0.5, ay=0.5 to center the text at the specified point.
	DrawStringAnchored(s string, x, y, ax, ay float64)
	Width() int
	Height() int
	DrawRectangle(x, y, w, h float64)
	Stroke()
	Clear()
	SetRGB(r, g, b float64)
}
type PixelDrawingContextI interface {
	Image() image.Image
}
type DrawingContextI interface {
	VectorDrawingContextI
	PixelDrawingContextI
	FontDrawingContextI
}

type HookFunc func(word string, x, y, w, h float64, col color.Color, size float64)

// Wordcloud object. Create one with NewWordcloud and use Draw() to get the image
type Wordcloud struct {
	sortedWordList *WordDataCoArrays

	grid *spatialHashMap
	dc   *gg.Context
	//dc              DrawingContextI
	randomPlacement bool
	width           float64
	height          float64
	opts            Options
	circles         map[float64]*circle
	fonts           map[float64]font.Face
	radii           []float64
	hook            HookFunc
}
type WordDataCoArrays struct {
	Word       []string
	Count      []int
	ColorIndex []uint16

	FontSize []float32
	Pos      []complex64
	Rect     []complex64
}

func NewWordDataCoArrays(estSize int) *WordDataCoArrays {
	return &WordDataCoArrays{
		Word:       make([]string, 0, estSize),
		Count:      make([]int, 0, estSize),
		ColorIndex: make([]uint16, 0, estSize),
		FontSize:   make([]float32, 0, estSize),
		Pos:        make([]complex64, 0, estSize),
		Rect:       make([]complex64, 0, estSize),
	}
}

var notPlacedPos = complex64(complex(math32.Inf(-1), float32(0.0)))

func (inst *WordDataCoArrays) Add(word string, count int, colorIndex uint16) {
	inst.Word = append(inst.Word, word)
	inst.Count = append(inst.Count, count)
	inst.ColorIndex = append(inst.ColorIndex, colorIndex)
}
func (inst *WordDataCoArrays) Prepare() {
	n := len(inst.Word)
	inst.FontSize = slices.Grow(inst.FontSize[:0], n)[:n]
	pos := slices.Grow(inst.Pos[:0], n)[:n]
	for i := 0; i < n; i++ {
		pos[i] = notPlacedPos
	}
	inst.Pos = pos
	inst.Rect = slices.Grow(inst.Rect[:0], n)[:n]
}
func (inst *WordDataCoArrays) Reset() {
	inst.Word = inst.Word[:0]
	inst.Count = inst.Count[:0]
	inst.ColorIndex = inst.ColorIndex[:0]
	inst.FontSize = inst.FontSize[:0]
	inst.Pos = inst.Pos[:0]
	inst.Rect = inst.Rect[:0]
}
func (inst *WordDataCoArrays) Length() int {
	return len(inst.Word)
}
func (inst *WordDataCoArrays) PlacedCount() (n int) {
	for _, p := range inst.Pos {
		if p != notPlacedPos {
			n++
		}
	}
	return
}
func (inst *WordDataCoArrays) ApplyColorPaletteSize(sz uint16) {
	ci := inst.ColorIndex
	for i, idx := range ci {
		ci[i] = idx % sz
	}
}
func (inst *WordDataCoArrays) AllPlaced() iter.Seq[int] {
	return func(yield func(int) bool) {
		for i, p := range inst.Pos {
			if p != notPlacedPos {
				if !yield(i) {
					break
				}
			}
		}
	}
}

func (inst *WordDataCoArrays) SortByCount() {
	co.CoSortSlicesReverse(inst.Count, func(i int, j int) {
		w := inst.Word
		w[j], w[i] = w[i], w[j]
		u := inst.ColorIndex
		u[j], u[i] = u[i], u[j]
	})
}

func NewWordcloud(sortedWordList *WordDataCoArrays, options ...Option) *Wordcloud {
	opts := defaultOptions
	for _, opt := range options {
		opt(&opts)
	}

	wordCountMaxInt := sortedWordList.Count[0]
	wordCountMax := float64(wordCountMaxInt)

	sortedWordList.Prepare()
	sizes := sortedWordList.FontSize
	m := wordCountMaxInt
	for idx, count := range sortedWordList.Count {
		if count > m {
			panic(fmt.Sprintf("not sorted: idx=%d, m=%d, count=%d", idx, m, count))
		}
		size := opts.SizeFunction(float64(count)/wordCountMax) * float64(opts.FontMaxSize)
		if size < float64(opts.FontMinSize) {
			size = float64(opts.FontMinSize)
		}
		sizes[idx] = float32(size)
		m = count
	}

	//var dc DrawingContextI
	var dc *gg.Context
	dc = gg.NewContext(opts.Width, opts.Height)
	dc.SetColor(opts.BackgroundColor)
	dc.Clear()
	dc.SetRGB(0, 0, 0)
	grid := newSpatialHashMap(float64(opts.Width), float64(opts.Height), opts.Height/10)

	for _, b := range opts.Mask {
		if opts.Debug {
			dc.DrawRectangle(b.x(), b.y(), b.w(), b.h())
			dc.Stroke()
		}
		grid.Add(b)
	}

	radius := 1.0
	maxRadius := math.Sqrt(float64(opts.Width*opts.Width + opts.Height*opts.Height))
	circles := make(map[float64]*circle)
	radii := make([]float64, 0)
	for radius < maxRadius {
		circles[radius] = newCircle(float64(opts.Width/2), float64(opts.Height/2), radius, 512)
		radii = append(radii, radius)
		radius = radius + 5.0
	}

	rand.Seed(time.Now().UnixNano())

	return &Wordcloud{
		sortedWordList:  sortedWordList,
		grid:            grid,
		dc:              dc,
		randomPlacement: opts.RandomPlacement,
		width:           float64(opts.Width),
		height:          float64(opts.Height),
		opts:            opts,
		circles:         circles,
		fonts:           make(map[float64]font.Face),
		radii:           radii,
	}
}
func (w *Wordcloud) SetHook(hook HookFunc) {
	w.hook = hook
}

func (w *Wordcloud) getPreciseBoundingBoxes(b *Box) []*Box {
	res := make([]*Box, 0)
	step := 5

	defColor := w.opts.BackgroundColor
	for i := int(math.Floor(b.Left)); i < int(b.Right); i = i + step {
		for j := int(b.Bottom); j < int(b.Top); j = j + step {
			if w.dc.Image().At(i, j) != defColor {
				res = append(res, &Box{
					float64(j+step) + 5,
					float64(i) - 5,
					float64(i+step) + 5,
					float64(j) - 5,
				})
			}
		}
	}
	return res
}

func (w *Wordcloud) setFont(size float64) {
	// TODO do not call w.dc.SetFontFace if previously set with same value
	size = math.Round(size) // FIXME linear
	f, ok := w.fonts[size]

	if !ok {
		var err error
		f, err = gg.LoadFontFace(w.opts.FontFile, size)
		if err != nil {
			panic(err)
		}
		if len(w.fonts) > 100 {
			panic("more than 100 distinct font sizes")
		} else {
			w.fonts[size] = f
		}
	}

	w.dc.SetFontFace(f)
}

func (w *Wordcloud) Place(idx int) bool {
	data := w.sortedWordList
	word := data.Word[idx]

	colors := w.opts.Colors
	c := colors[int(data.ColorIndex[idx])%len(colors)]
	w.dc.SetColor(c)
	w.setFont(float64(data.FontSize[idx]))

	width, height := w.dc.MeasureString(word)

	width += 5
	height += 5
	x, y, space := w.nextPos(width, height)
	if !space {
		data.Pos[idx] = notPlacedPos
		data.Rect[idx] = 0.0
		return false
	}
	const ax = 0.5
	const ay = 0.5
	w.dc.DrawStringAnchored(word, x, y, ax, ay)
	data.Pos[idx] = complex(float32(x-ax*(width-5)), float32(y-ay*(height-5)))
	data.Rect[idx] = complex(float32(width-5), float32(height-5))

	box := &Box{
		y + height/2 + 0.3*height,
		x - width/2,
		x + width/2,
		math.Max(y-height/2, 0),
	}
	if height > 40 {
		preciseBoxes := w.getPreciseBoundingBoxes(box)
		for _, pb := range preciseBoxes {
			w.grid.Add(pb)
			if w.opts.Debug {
				w.dc.DrawRectangle(pb.x(), pb.y(), pb.w(), pb.h())
				w.dc.Stroke()
			}
		}
	} else {
		w.grid.Add(box)
	}
	return true
}

// Draw tries to place words one by one, starting with the ones with the highest counts
func (w *Wordcloud) Draw() image.Image {
	consecutiveMisses := 0
	l := w.sortedWordList.Length()
	for i := 0; i < l; i++ {
		success := w.Place(i)
		if !success {
			consecutiveMisses++
			if consecutiveMisses > 10 {
				return w.dc.Image()
			}
			continue
		}
		consecutiveMisses = 0
	}
	return w.dc.Image()
}

func (w *Wordcloud) nextRandom(width float64, height float64) (x float64, y float64, space bool) {
	tries := 0
	searching := true
	var box Box
	for searching && tries < 5000000 {
		tries++
		x, y = float64(rand.Intn(w.dc.Width())), float64(rand.Intn(w.dc.Height()))
		// Is that position available?
		box.Top = y + height/2
		box.Left = x - width/2
		box.Right = x + width/2
		box.Bottom = y - height/2

		if !box.fits(w.width, w.height) {
			continue
		}
		colliding, _ := w.grid.TestCollision(&box, func(a *Box, b *Box) bool {
			return a.overlaps(b)
		})

		if !colliding {
			space = true
			searching = false
			return
		}
	}
	return
}

// Data sent to placement workers
type workerData struct {
	radius    float64
	positions []point
	width     float64
	height    float64
}

// Results sent from placement workers
type res struct {
	radius float64
	x      float64
	y      float64
	failed bool
}

// Multithreaded word placement
func (w *Wordcloud) nextPos(width float64, height float64) (x float64, y float64, space bool) {
	if w.randomPlacement {
		return w.nextRandom(width, height)
	}

	space = false

	x, y = w.width, w.height

	stopSendingCh := make(chan struct{}, 1)
	aggCh := make(chan res, 100)
	workCh := make(chan workerData, runtime.NumCPU())
	results := make(map[float64]res)
	done := make(map[float64]bool)
	stopChannels := make([]chan struct{}, 0)
	wg := sync.WaitGroup{}

	// Start workers that will test each one "circle" of positions
	for i := 0; i < runtime.NumCPU(); i++ {
		wg.Add(1)
		stopCh := make(chan struct{}, 1)
		go func(ch chan struct{}, i int) {
			defer wg.Done()
			for {
				select {
				// Receive data
				case d, ok := <-workCh:
					if !ok {
						return
					}
					// Test the positions and post results on aggCh
					aggCh <- w.testRadius(d.radius, d.positions, d.width, d.height)
				case <-ch:
					// Stop signal
					return
				}
			}
		}(stopCh, i)
		stopChannels = append(stopChannels, stopCh)
	}

	// Post positions to test to worker channel
	go func() {
		for _, r := range w.radii {
			c := w.circles[r]
			select {
			case <-stopSendingCh:
				// Stop sending data immediately if a position has already been found
				close(workCh)
				return
			case workCh <- workerData{
				radius:    r,
				positions: c.positions(),
				width:     width,
				height:    height,
			}:
			}
		}
		// Close channel after all positions have been sent
		close(workCh)
	}()

	defer func() {
		// Stop data sending
		stopSendingCh <- struct{}{}
		// Tell the worker goroutines to stop
		for _, c := range stopChannels {
			c <- struct{}{}
		}
		// Purge res channel in case some workers are still sending data
		go func() {
			for {
				select {
				case <-aggCh:
				default:
					return
				}
			}
		}()

		// Wait for all goroutines to stop. We want to wait for them so that no thread is accessing internal data structs
		// such as the spatial hashmap
		wg.Wait()
	}()

	// Finally, aggregate the results coming from workers
	for d := range aggCh {
		results[d.radius] = d
		done[d.radius] = true
		//check if we need to continue
		failed := true
		// Example: if we know that there's a successful placement at r=10 but have not received results for r=5,
		// we need to wait as there might be a closer successful position
		for _, r := range w.radii {
			if !done[r] {
				// Some positions are not done. They might be successful
				failed = false
				break
			}
			// We have the successful placement with the lowest radius
			if !results[r].failed {
				return results[r].x, results[r].y, true
			}
		}

		// We tried it all but could not place the word
		if failed {
			return
		}

	}
	return
}

// test a series of points on a circle and returns as soon as there's a match
func (w *Wordcloud) testRadius(radius float64, points []point, width float64, height float64) res {
	var box Box
	var x, y float64

	for _, p := range points {
		y = p.y
		x = p.x

		// Is that position available?
		box.Top = y + height/2
		box.Left = x - width/2
		box.Right = x + width/2
		box.Bottom = y - height/2

		if !box.fits(w.width, w.height) {
			continue
		}
		colliding, _ := w.grid.TestCollision(&box, func(a *Box, b *Box) bool {
			return a.overlaps(b)
		})

		if !colliding {
			return res{
				x:      x,
				y:      y,
				failed: false,
				radius: radius,
			}
		}
	}
	return res{
		x:      x,
		y:      y,
		failed: true,
		radius: radius,
	}
}
