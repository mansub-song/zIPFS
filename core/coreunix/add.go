package coreunix

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	gopath "path"
	"strconv"
	"time"

	"github.com/ipfs/go-cid"
	bstore "github.com/ipfs/go-ipfs-blockstore"
	chunker "github.com/ipfs/go-ipfs-chunker"
	files "github.com/ipfs/go-ipfs-files"
	pin "github.com/ipfs/go-ipfs-pinner"
	posinfo "github.com/ipfs/go-ipfs-posinfo"
	ipld "github.com/ipfs/go-ipld-format"
	logging "github.com/ipfs/go-log"
	"github.com/ipfs/go-merkledag"
	dag "github.com/ipfs/go-merkledag"
	"github.com/ipfs/go-mfs"
	"github.com/ipfs/go-unixfs"
	"github.com/ipfs/go-unixfs/importer/balanced"
	ihelper "github.com/ipfs/go-unixfs/importer/helpers"
	"github.com/ipfs/go-unixfs/importer/trickle"
	coreiface "github.com/ipfs/interface-go-ipfs-core"
	"github.com/ipfs/interface-go-ipfs-core/path"
)

var log = logging.Logger("coreunix")

var Read_elap int = 0

// how many bytes of progress to wait before sending a progress update message
const progressReaderIncrement = 1024 * 256

var liveCacheSize = uint64(256 << 10)

type Link struct {
	Name, Hash string
	Size       uint64
}

type syncer interface {
	Sync() error
}

// NewAdder Returns a new Adder used for a file add operation.
func NewAdder(ctx context.Context, p pin.Pinner, bs bstore.GCLocker, ds ipld.DAGService) (*Adder, error) {
	bufferedDS := ipld.NewBufferedDAG(ctx, ds)

	return &Adder{
		ctx:        ctx,
		pinning:    p,
		gcLocker:   bs,
		dagService: ds,
		bufferedDS: bufferedDS,
		Progress:   false,
		Pin:        true,
		Trickle:    false,
		Chunker:    "",
	}, nil
}

// Adder holds the switches passed to the `add` command.
type Adder struct {
	ctx        context.Context
	pinning    pin.Pinner
	gcLocker   bstore.GCLocker
	dagService ipld.DAGService
	bufferedDS *ipld.BufferedDAG
	Out        chan<- interface{}
	Progress   bool
	Pin        bool
	Trickle    bool
	RawLeaves  bool
	Silent     bool
	NoCopy     bool
	Chunker    string
	mroot      *mfs.Root
	unlocker   bstore.Unlocker
	tempRoot   cid.Cid
	CidBuilder cid.Builder
	liveNodes  uint64
}

func (adder *Adder) mfsRoot() (*mfs.Root, error) {
	if adder.mroot != nil {
		fmt.Println("mssong_here_1000")
		return adder.mroot, nil
	}
	rnode := unixfs.EmptyDirNode()
	rnode.SetCidBuilder(adder.CidBuilder)

	if merkledag.IsMfsRootVisit == false {
		merkledag.IsMfsRootVisit = true
		merkledag.FileCidMap[rnode.Cid()] = append(merkledag.FileCidMap[rnode.Cid()], rnode.Cid())
		merkledag.InitCidArr = append(merkledag.InitCidArr, rnode.Cid())
		fmt.Println("mfsRoot rnode:", rnode.Cid().String())
	}
	mr, err := mfs.NewRoot(adder.ctx, adder.dagService, rnode, nil)
	if err != nil {
		return nil, err
	}
	adder.mroot = mr
	return adder.mroot, nil
}

// SetMfsRoot sets `r` as the root for Adder.
func (adder *Adder) SetMfsRoot(r *mfs.Root) {
	adder.mroot = r
}

// Constructs a node from reader's data, and adds it. Doesn't pin.
func (adder *Adder) add(reader io.Reader) (ipld.Node, error) {
	// debug.PrintStack()

	chnk, err := chunker.FromString(reader, adder.Chunker)
	// fmt.Printf("adder.Chunker:%v\n", adder.Chunker) //size-262144
	// fmt.Printf("chnk:%v\n", chnk)

	if err != nil {
		return nil, err
	}

	params := ihelper.DagBuilderParams{
		Dagserv:    adder.bufferedDS,
		RawLeaves:  adder.RawLeaves,
		Maxlinks:   ihelper.DefaultLinksPerBlock,
		NoCopy:     adder.NoCopy,
		CidBuilder: adder.CidBuilder,
	}
	db, err := params.New(chnk) // db = DagBuilderHelper
	// fmt.Printf("db = %#v\n\n", db)
	if err != nil {
		return nil, err
	}
	var nd ipld.Node
	// fmt.Printf("adder.Trickle:%v\n", adder.Trickle) //false

	if adder.Trickle {
		nd, err = trickle.Layout(db)
	} else {
		start := time.Now()
		nd, err = balanced.Layout(db)
		elapse := time.Since(start)
		fmt.Println("Layout elapse:", elapse)

	}
	if err != nil {
		return nil, err
	}
	// FileCidLength := len(merkledag.FileCid)
	// for i := 0; i < FileCidLength; i++ {
	// 	fmt.Println("FileCid:", merkledag.FileCid[i])
	// }

	fmt.Println("adder.bufferedDS.Commit()")
	return nd, adder.bufferedDS.Commit()
}

// RootNode returns the mfs root node
func (adder *Adder) curRootNode() (ipld.Node, error) {
	mr, err := adder.mfsRoot()
	if err != nil {
		return nil, err
	}
	root, err := mr.GetDirectory().GetNode()
	if err != nil {
		return nil, err
	}

	// if one root file, use that hash as root.
	if len(root.Links()) == 1 {
		nd, err := root.Links()[0].GetNode(adder.ctx, adder.dagService)
		if err != nil {
			return nil, err
		}

		root = nd
	}

	return root, err
}

// Recursively pins the root node of Adder and
// writes the pin state to the backing datastore.
func (adder *Adder) PinRoot(root ipld.Node) error {
	// debug.PrintStack()
	if !adder.Pin {
		return nil
	}

	rnk := root.Cid()

	err := adder.dagService.Add(adder.ctx, root)
	if err != nil {
		return err
	}

	if adder.tempRoot.Defined() {
		err := adder.pinning.Unpin(adder.ctx, adder.tempRoot, true)
		if err != nil {
			return err
		}
		adder.tempRoot = rnk
	}

	adder.pinning.PinWithMode(rnk, pin.Recursive)
	return adder.pinning.Flush(adder.ctx)
}

func (adder *Adder) outputDirs(path string, fsn mfs.FSNode) error {
	switch fsn := fsn.(type) {
	case *mfs.File:
		return nil
	case *mfs.Directory:
		names, err := fsn.ListNames(adder.ctx)
		if err != nil {
			return err
		}

		for _, name := range names {
			child, err := fsn.Child(name)
			if err != nil {
				return err
			}

			childpath := gopath.Join(path, name)
			err = adder.outputDirs(childpath, child)
			if err != nil {
				return err
			}

			fsn.Uncache(name)
		}
		nd, err := fsn.GetNode()
		if err != nil {
			return err
		}
		err = outputDagnode(adder.Out, path, nd)
		return err
	default:
		return fmt.Errorf("unrecognized fsn type: %#v", fsn)
	}
}

func (adder *Adder) addNode(node ipld.Node, path string) error {
	// patch it into the root
	// fmt.Printf("path: %v\n", path) // ""
	if path == "" {
		path = node.Cid().String()
	}
	// fmt.Printf("path: %v\n", path) //path: QmcwUFZGxNvZMkCUbhhjx43vLJz79CbxyPRUP3roJ3bQMi

	if pi, ok := node.(*posinfo.FilestoreNode); ok {
		node = pi.Node
	}
	mr, err := adder.mfsRoot()
	if err != nil {
		return err
	}
	dir := gopath.Dir(path)
	if dir != "." {
		opts := mfs.MkdirOpts{
			Mkparents:  true,
			Flush:      false,
			CidBuilder: adder.CidBuilder,
		}
		if err := mfs.Mkdir(mr, dir, opts); err != nil {
			return err
		}
	}

	if err := mfs.PutNode(mr, path, node); err != nil {
		return err
	}
	fmt.Println("mssong_here_1")
	if !adder.Silent {
		err = outputDagnode(adder.Out, path, node)
		return err
	}
	fmt.Println("mssong_here_1_1")
	return nil
}

// AddAllAndPin adds the given request's files and pin them.
func (adder *Adder) AddAllAndPin(ctx context.Context, file files.Node) (ipld.Node, error) {
	if adder.Pin { //true
		adder.unlocker = adder.gcLocker.PinLock(ctx)
	}
	defer func() {
		if adder.unlocker != nil {
			adder.unlocker.Unlock(ctx)
		}
	}()

	if err := adder.addFileNode(ctx, "", file, true); err != nil {
		fmt.Println("mssong_here_100")
		return nil, err
	}
	fmt.Println("mssong_here_101")

	// get root
	mr, err := adder.mfsRoot()
	if err != nil {
		return nil, err
	}
	fmt.Println("mssong_here_102")
	var root mfs.FSNode
	rootdir := mr.GetDirectory()
	root = rootdir

	err = root.Flush()
	if err != nil {
		return nil, err
	}
	fmt.Println("mssong_here_103")

	// if adding a file without wrapping, swap the root to it (when adding a
	// directory, mfs root is the directory)
	_, dir := file.(files.Directory)
	var name string
	if !dir {
		children, err := rootdir.ListNames(adder.ctx)
		if err != nil {
			return nil, err
		}
		fmt.Println("mssong_here_104")
		if len(children) == 0 {
			return nil, fmt.Errorf("expected at least one child dir, got none")
		}
		fmt.Println("mssong_here_104_1")
		// Replace root with the first child
		name = children[0]
		fmt.Println("name:", name) //name: QmQy6xmJhrcC5QLboAcGFcAE1tC8CrwDVkrHdEYJkLscrQ
		// time.Sleep(3 * time.Second)
		root, err = rootdir.Child(name) //here unmarshal 여기가 에러!!!

		if err != nil {
			return nil, err
		}
		fmt.Println("mssong_here_105")
	}

	// nd, err := rootdir.Child_mansub(name)

	err = mr.Close()
	if err != nil {
		return nil, err
	}

	nd, err := root.GetNode()
	if err != nil {
		return nil, err
	}
	// fmt.Printf("root:%#v\n", root) //root:&mfs.File
	// fmt.Printf("nd:%#v\n", nd)     //nd:&merkledag.ProtoNode
	// time.Sleep(3 * time.Second)
	fmt.Println("mssong_here_106")

	// output directory events
	err = adder.outputDirs(name, root)
	if err != nil {
		return nil, err
	}
	fmt.Println("mssong_here_107")

	if asyncDagService, ok := adder.dagService.(syncer); ok {
		err = asyncDagService.Sync()
		if err != nil {
			return nil, err
		}
	}

	// if !adder.Pin {
	// 	return nd, nil
	// }

	fmt.Println("Read_elap:", time.Duration(Read_elap))
	return nd, adder.PinRoot(nd)
}

func (adder *Adder) addFileNode(ctx context.Context, path string, file files.Node, toplevel bool) error {
	defer file.Close()

	err := adder.maybePauseForGC(ctx)
	if err != nil {
		return err
	}

	// fmt.Println("adder.livenodes", adder.liveNodes, liveCacheSize)
	if adder.liveNodes >= liveCacheSize { // adder.liveNodes:0, liveCacheSize:262144
		// TODO: A smarter cache that uses some sort of lru cache with an eviction handler
		mr, err := adder.mfsRoot()
		if err != nil {
			return err
		}
		if err := mr.FlushMemFree(adder.ctx); err != nil {
			return err
		}

		adder.liveNodes = 0
	}
	adder.liveNodes++

	switch f := file.(type) {
	case files.Directory:
		return adder.addDir(ctx, path, f, toplevel)
	case *files.Symlink:
		return adder.addSymlink(path, f)
	case files.File:
		return adder.addFile(path, f)
	default:
		return errors.New("unknown file type")
	}
}

func (adder *Adder) addSymlink(path string, l *files.Symlink) error {
	sdata, err := unixfs.SymlinkData(l.Target)
	if err != nil {
		return err
	}

	dagnode := dag.NodeWithData(sdata)
	dagnode.SetCidBuilder(adder.CidBuilder)
	err = adder.dagService.Add(adder.ctx, dagnode)
	if err != nil {
		return err
	}

	return adder.addNode(dagnode, path)
}

func (adder *Adder) addFile(path string, file files.File) error {
	// debug.PrintStack()
	// fmt.Println("path:", path)
	// fmt.Printf("file:%v\n", file)
	// if the progress flag was specified, wrap the file so that we can send
	// progress updates to the client (over the output channel)
	var reader io.Reader = file

	if fi, ok := file.(files.FileInfo); ok {
		// fmt.Printf("fi: %#v\n", fi.AbsPath())
		balanced.FileAbsPath = fi.AbsPath()
		var err error
		if fi.AbsPath() != "" {
			chunker.OsFile, err = os.Open(fi.AbsPath())
			if err != nil {
				log.Fatal("os open fail!")
			}
		}

	}

	// fmt.Printf("file: %#v\n", file)
	// fmt.Printf("path: %#v\n", path)
	//////////////////////////
	// abspath := file.AbsPath()
	// if data, err := os.Stat(abspath); os.IsNotExist(err) {
	// 	fmt.Println("Log.txt file is not")
	// } else {

	// 	fmt.Println(data.Name())
	// 	fmt.Println(data.Size())
	// 	fmt.Println(data.Mode())
	// 	fmt.Println(data.ModTime())
	// 	fmt.Println(data.IsDir())
	// 	fmt.Println(data.Sys())
	// }
	////////////////////////////
	// fmt.Println("adder.Progress:", adder.Progress)
	if adder.Progress { //true
		rdr := &progressReader{file: reader, path: path, out: adder.Out}
		if fi, ok := file.(files.FileInfo); ok {
			reader = &progressReader2{rdr, fi}
		} else {
			reader = rdr
		}
	}
	dagnode, err := adder.add(reader) //여기서 cid값 정하는 듯!
	// fmt.Printf("dagnode:%v\n", dagnode)
	if err != nil {
		return err
	}
	// patch it into the root
	return adder.addNode(dagnode, path)
}

func (adder *Adder) addDir(ctx context.Context, path string, dir files.Directory, toplevel bool) error {
	log.Infof("adding directory: %s", path)

	if !(toplevel && path == "") {
		mr, err := adder.mfsRoot()
		if err != nil {
			return err
		}
		err = mfs.Mkdir(mr, path, mfs.MkdirOpts{
			Mkparents:  true,
			Flush:      false,
			CidBuilder: adder.CidBuilder,
		})
		if err != nil {
			return err
		}
	}

	it := dir.Entries()
	for it.Next() {
		fpath := gopath.Join(path, it.Name())
		err := adder.addFileNode(ctx, fpath, it.Node(), false)
		if err != nil {
			return err
		}
	}

	return it.Err()
}

func (adder *Adder) maybePauseForGC(ctx context.Context) error {

	// fmt.Printf("adder.unlocker:%v\n", adder.unlocker)
	// fmt.Printf("adder.gcLocker.GCRequested(ctx):%v\n", adder.gcLocker.GCRequested(ctx)) //false
	if adder.unlocker != nil && adder.gcLocker.GCRequested(ctx) {
		// fmt.Println("here?")
		rn, err := adder.curRootNode()
		if err != nil {
			return err
		}

		err = adder.PinRoot(rn)
		if err != nil {
			return err
		}

		adder.unlocker.Unlock(ctx)
		adder.unlocker = adder.gcLocker.PinLock(ctx)
	}
	return nil
}

// outputDagnode sends dagnode info over the output channel
func outputDagnode(out chan<- interface{}, name string, dn ipld.Node) error {
	fmt.Println("mssong_here_22")
	if out == nil {
		return nil
	}

	o, err := getOutput(dn)
	fmt.Println("mssong_here_2")

	if err != nil {
		return err
	}
	fmt.Println("mssong_here_3")

	out <- &coreiface.AddEvent{
		Path: o.Path,
		Name: name,
		Size: o.Size,
	}

	return nil
}

// from core/commands/object.go
func getOutput(dagnode ipld.Node) (*coreiface.AddEvent, error) {
	fmt.Println("mssong_here_4")
	c := dagnode.Cid()
	fmt.Println("mssong_here_5")
	s, err := dagnode.Size()
	fmt.Println("mssong_here_6")
	if err != nil {
		return nil, err
	}

	output := &coreiface.AddEvent{
		Path: path.IpfsPath(c),
		Size: strconv.FormatUint(s, 10),
	}

	return output, nil
}

type progressReader struct {
	file         io.Reader
	path         string
	out          chan<- interface{}
	bytes        int64
	lastProgress int64
}

func (i *progressReader) Read(p []byte) (int, error) {
	st := time.Now()
	n, err := i.file.Read(p)
	// n := len(p)
	// var err error = nil

	i.bytes += int64(n)
	if i.bytes-i.lastProgress >= progressReaderIncrement || err == io.EOF {
		i.lastProgress = i.bytes
		// i.out <- &coreiface.AddEvent{ // mssong - 여기가 event생성 되는 곳!!
		// 	Name:  i.path,
		// 	Bytes: i.bytes,
		// }
	}
	elap := time.Since(st)
	Read_elap = Read_elap + int(elap)
	// fmt.Println("read elap:", elap)

	return n, err
}

type progressReader2 struct {
	*progressReader
	files.FileInfo
}

func (i *progressReader2) Read(p []byte) (int, error) {
	return i.progressReader.Read(p)
}
