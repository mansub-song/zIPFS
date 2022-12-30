// Package gc provides garbage collection for go-ipfs.
package gc

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	bserv "github.com/ipfs/go-blockservice"
	cid "github.com/ipfs/go-cid"
	dstore "github.com/ipfs/go-datastore"
	bstore "github.com/ipfs/go-ipfs-blockstore"
	dshelp "github.com/ipfs/go-ipfs-ds-help"
	offline "github.com/ipfs/go-ipfs-exchange-offline"
	pin "github.com/ipfs/go-ipfs-pinner"
	ipld "github.com/ipfs/go-ipld-format"
	logging "github.com/ipfs/go-log"
	dag "github.com/ipfs/go-merkledag"
	"github.com/ipfs/go-verifcid"
)

var log = logging.Logger("gc")
var HasTime = 0

// var numThread = 32
// var IPFS_Path = "/home/mssong/.ipfs"

// Result represents an incremental output from a garbage collection
// run.  It contains either an error, or the cid of a removed object.
type Result struct {
	KeyRemoved cid.Cid
	Error      error
}

// converts a set of CIDs with different codecs to a set of CIDs with the raw codec.
func toRawCids(set *cid.Set) (*cid.Set, error) {
	newSet := cid.NewSet()
	err := set.ForEach(func(c cid.Cid) error {
		newSet.Add(cid.NewCidV1(cid.Raw, c.Hash()))
		return nil
	})
	return newSet, err
}

func OptColoredSet() *cid.Set {
	gcs := cid.NewSet()

	// fmt.Printf("OptColoredSet Map:%+v\n", dag.FileCidMap)
	// fmt.Printf("dag.FileCidMap:%+v\n", dag.FileCidMap)
	for _, tieredCID := range dag.PinBuffer {
		for _, cid := range tieredCID.NonLeaf {
			// fmt.Printf("OptColoredSet_cid:%v\n", cid)
			// fmt.Printf("OptColoredSet_cidV1:%v\n", toCidV1(cid))
			gcs.Visit(toCidV1(cid))
		}
		for _, cid := range tieredCID.Leaf {
			// fmt.Printf("OptColoredSet_cid:%v\n", cid)
			// fmt.Printf("OptColoredSet_cidV1:%v\n", toCidV1(cid))
			gcs.Visit(toCidV1(cid))
		}
	}

	// for _, val := range dag.UnpinnedCidMap {
	// 	for _, cid := range val {
	// 		gcs.Visit(toCidV1(cid))
	// 	}
	// }
	fmt.Println("gcs.Len():", gcs.Len())
	return gcs
}

func encode(key dstore.Key) (dirPath, filePath string) {
	// dir: /home/mssong/.ipfs/blocks/7P path: /home/mssong/.ipfs/blocks/7P/CIQKGXY65BAIM2G5C64GRJOK2SGPDAXNG5VGHVHW7KFQ3PFFF5YH7PI.data
	extension := ".data"
	noslash := key.String()[1:]
	// datastorePath := "/home/mssong/.ipfs/blocks"
	datastorePath := dag.IPFS_Path + "/blocks"

	dirPath = filepath.Join(datastorePath, noslash[len(noslash)-3:len(noslash)-1])
	filePath = filepath.Join(dirPath, noslash+extension)
	return dirPath, filePath
}

func removeSet(gcs *cid.Set, keys []cid.Cid, ctx context.Context, bs bstore.GCBlockstore, output chan Result) {
	// removeKeys := make([]cid.Cid, 0)
	fmt.Println("keys length:", len(keys))
	if len(keys) > 0 {
		for _, key := range keys {
			st := time.Now()
			has := gcs.Has(key)
			elap := time.Since(st)
			HasTime = HasTime + int(elap)
			if !has {
				/////
				// err := bs.DeleteBlock(ctx, key)
				////
				dsKey := dshelp.MultihashToDsKey(key.Hash())
				_, filePath := encode(dsKey)
				// fmt.Println("filePath:", filePath)
				err := os.Remove(filePath)
				if err != nil {
					select {
					case output <- Result{Error: &CannotDeleteBlockError{key, err}}:
					case <-ctx.Done():
						println("break loop_1")
						return
					}
					// continue as error is non-fatal
					println("continue loop@@@")
					continue
				}
				select {
				case output <- Result{KeyRemoved: key}:
				case <-ctx.Done():
					println("break loop_2")
					break
				}
			}
		}
	}
	// return removeKeys

}
func parallelRemoveSet(gcs *cid.Set, allkeys []cid.Cid, numTh int, ctx context.Context, bs bstore.GCBlockstore, output chan Result) {
	// removeKeys := make([]cid.Cid, numTh)
	if len(allkeys) < numTh {
		removeSet(gcs, allkeys, ctx, bs, output)
	} else {
		size := len(allkeys) / numTh
		var wg sync.WaitGroup

		for i := 0; i < numTh; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				begin, end := i*size, (i+1)*size
				if end > len(allkeys) {
					end = len(allkeys)
				}
				removeSet(gcs, allkeys[begin:end], ctx, bs, output)
			}(i)
		}
		wg.Wait()
	}
}

// GC performs a mark and sweep garbage collection of the blocks in the blockstore
// first, it creates a 'marked' set and adds to it the following:
// - all recursively pinned blocks, plus all of their descendants (recursively)
// - bestEffortRoots, plus all of its descendants (recursively)
// - all directly pinned blocks
// - all blocks utilized internally by the pinner
//
// The routine then iterates over every block in the blockstore and
// deletes any block that is not found in the marked set.
func GC(ctx context.Context, bs bstore.GCBlockstore, dstor dstore.Datastore, pn pin.Pinner, bestEffortRoots []cid.Cid) <-chan Result {
	fmt.Println("@@GC start")
	HasTime = 0
	ctx, cancel := context.WithCancel(ctx)

	unlocker := bs.GCLock(ctx)

	bsrv := bserv.New(bs, offline.Exchange(bs))
	ds := dag.NewDAGService(bsrv)

	output := make(chan Result, 128)

	go func() {
		var err error
		defer cancel()
		defer close(output)
		defer unlocker.Unlock(ctx)

		gcOptFlag := true
		if gcOptFlag {
			startTime := time.Now()
			gcsOpt := OptColoredSet() //optimization
			// gcsOpt, err := ColoredSet(ctx, pn, ds, bestEffortRoots, output) // traditional
			if err != nil {
				select {
				case output <- Result{Error: err}:
				case <-ctx.Done():
				}
				return
			}
			gcsOpt, err = toRawCids(gcsOpt)

			if err != nil {
				select {
				case output <- Result{Error: err}:
				case <-ctx.Done():
				}
				return
			}
			elapsedTime := time.Since(startTime)
			fmt.Printf("######OptColoredSet time: %vms\n\n", elapsedTime.Milliseconds())
			// fmt.Fprintf(f, "OptColoredSet time: %v\n", elapsedTime.Milliseconds())
			fmt.Printf("gcsOpt:%+v\n\n", gcsOpt.Len())

			// bigInt := new(big.Int)
			// bigInt.SetInt64(elapsedTime.Milliseconds())
			// bigInt.Bytes()

			// f.WriteString("text to append\n")
			// if _, err := f.Write([]byte("\nOptColoredSet time: ")); err != nil {
			// 	log.Fatal(err)
			// }
			// if _, err := f.Write(bigInt.Bytes()); err != nil {
			// 	log.Fatal(err)
			// }

			// Here !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
			startTime = time.Now()
			startTimAllKeys := time.Now()
			allKeys, err := bs.AllKeysMansub(ctx)
			elapsedAllKeys := time.Since(startTimAllKeys)
			fmt.Printf("ONLY AllKeysMansub time: %vms\n", elapsedAllKeys.Milliseconds())
			if err != nil {
				select {
				case output <- Result{Error: err}:
				case <-ctx.Done():

				}
				return
			}
			// st := time.Now()
			// removeSet(gcsOpt, allKeys, ctx, bs, output)
			// elap := time.Since(st)
			// fmt.Println("HasTime:", time.Duration(HasTime))
			// fmt.Println("removeSet time:", elap)

			parallelRemoveSet(gcsOpt, allKeys, dag.NumThread, ctx, bs, output)
			elapsedTime = time.Since(startTime)
			fmt.Printf("######Delete Block time(dag.NumThread = %d): %vms\n", dag.NumThread, elapsedTime.Milliseconds())
			// fmt.Fprintf(f, "Delete Block time(numThread = %d): %v\n", numThread, elapsedTime.Milliseconds())

		} else {
			gcs, err := ColoredSet(ctx, pn, ds, bestEffortRoots, output)
			if err != nil {
				select {
				case output <- Result{Error: err}:
				case <-ctx.Done():
				}
				return
			}

			// The blockstore reports raw blocks. We need to remove the codecs from the CIDs.
			gcs, err = toRawCids(gcs)
			if err != nil {
				select {
				case output <- Result{Error: err}:
				case <-ctx.Done():
				}
				return
			}

			keychan, err := bs.AllKeysChan(ctx)
			if err != nil {
				select {
				case output <- Result{Error: err}:
				case <-ctx.Done():
				}
				return
			}

			errors := false
			var removed uint64

		loop:
			for ctx.Err() == nil { // select may not notice that we're "done".
				select {
				case k, ok := <-keychan:
					if !ok {
						break loop
					}
					// NOTE: assumes that all CIDs returned by the keychan are _raw_ CIDv1 CIDs.
					// This means we keep the block as long as we want it somewhere (CIDv1, CIDv0, Raw, other...).
					if !gcs.Has(k) {
						err := bs.DeleteBlock(ctx, k)
						removed++
						if err != nil {
							errors = true
							select {
							case output <- Result{Error: &CannotDeleteBlockError{k, err}}:
							case <-ctx.Done():
								break loop
							}
							// continue as error is non-fatal
							continue loop
						}
						select {
						case output <- Result{KeyRemoved: k}:
						case <-ctx.Done():
							break loop
						}
					}
				case <-ctx.Done():
					break loop
				}
			}
			if errors {
				select {
				case output <- Result{Error: ErrCannotDeleteSomeBlocks}:
				case <-ctx.Done():
					return
				}
			}

		}
		gds, ok := dstor.(dstore.GCDatastore)
		if !ok {
			return
		}
		err = gds.CollectGarbage(ctx)
		if err != nil {
			select {
			case output <- Result{Error: err}:
			case <-ctx.Done():
			}
			return
		}
	}()

	return output
}

// Descendants recursively finds all the descendants of the given roots and
// adds them to the given cid.Set, using the provided dag.GetLinks function
// to walk the tree.
func Descendants(ctx context.Context, getLinks dag.GetLinks, set *cid.Set, roots []cid.Cid) error {
	verifyGetLinks := func(ctx context.Context, c cid.Cid) ([]*ipld.Link, error) {
		err := verifcid.ValidateCid(c)
		if err != nil {
			return nil, err
		}

		return getLinks(ctx, c)
	}

	verboseCidError := func(err error) error {
		if strings.Contains(err.Error(), verifcid.ErrBelowMinimumHashLength.Error()) ||
			strings.Contains(err.Error(), verifcid.ErrPossiblyInsecureHashFunction.Error()) {
			err = fmt.Errorf("\"%s\"\nPlease run 'ipfs pin verify'"+
				" to list insecure hashes. If you want to read them,"+
				" please downgrade your go-ipfs to 0.4.13\n", err)
			log.Error(err)
		}
		return err
	}

	for _, c := range roots {
		// Walk recursively walks the dag and adds the keys to the given set
		err := dag.Walk(ctx, verifyGetLinks, c, func(k cid.Cid) bool {
			return set.Visit(toCidV1(k))
		}, dag.Concurrent())

		if err != nil {
			err = verboseCidError(err)
			return err
		}
	}

	return nil
}

// toCidV1 converts any CIDv0s to CIDv1s.
func toCidV1(c cid.Cid) cid.Cid {
	if c.Version() == 0 {
		return cid.NewCidV1(c.Type(), c.Hash())
	}
	return c
}

// ColoredSet computes the set of nodes in the graph that are pinned by the
// pins in the given pinner.
func ColoredSet(ctx context.Context, pn pin.Pinner, ng ipld.NodeGetter, bestEffortRoots []cid.Cid, output chan<- Result) (*cid.Set, error) {
	// KeySet currently implemented in memory, in the future, may be bloom filter or
	// disk backed to conserve memory.
	errors := false
	gcs := cid.NewSet()
	getLinks := func(ctx context.Context, cid cid.Cid) ([]*ipld.Link, error) {
		links, err := ipld.GetLinks(ctx, ng, cid)
		if err != nil {
			errors = true
			select {
			case output <- Result{Error: &CannotFetchLinksError{cid, err}}:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		return links, nil
	}
	rkeys, err := pn.RecursiveKeys(ctx)
	if err != nil {
		return nil, err
	}
	err = Descendants(ctx, getLinks, gcs, rkeys)
	if err != nil {
		errors = true
		select {
		case output <- Result{Error: err}:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	bestEffortGetLinks := func(ctx context.Context, cid cid.Cid) ([]*ipld.Link, error) {
		links, err := ipld.GetLinks(ctx, ng, cid)
		if err != nil && err != ipld.ErrNotFound {
			errors = true
			select {
			case output <- Result{Error: &CannotFetchLinksError{cid, err}}:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		return links, nil
	}
	err = Descendants(ctx, bestEffortGetLinks, gcs, bestEffortRoots)
	if err != nil {
		errors = true
		select {
		case output <- Result{Error: err}:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	dkeys, err := pn.DirectKeys(ctx)
	if err != nil {
		return nil, err
	}
	for _, k := range dkeys {
		gcs.Add(toCidV1(k))
	}

	ikeys, err := pn.InternalPins(ctx)
	if err != nil {
		return nil, err
	}
	err = Descendants(ctx, getLinks, gcs, ikeys)
	if err != nil {
		errors = true
		select {
		case output <- Result{Error: err}:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	if errors {
		return nil, ErrCannotFetchAllLinks
	}

	return gcs, nil
}

// ErrCannotFetchAllLinks is returned as the last Result in the GC output
// channel when there was an error creating the marked set because of a
// problem when finding descendants.
var ErrCannotFetchAllLinks = errors.New("garbage collection aborted: could not retrieve some links")

// ErrCannotDeleteSomeBlocks is returned when removing blocks marked for
// deletion fails as the last Result in GC output channel.
var ErrCannotDeleteSomeBlocks = errors.New("garbage collection incomplete: could not delete some blocks")

// CannotFetchLinksError provides detailed information about which links
// could not be fetched and can appear as a Result in the GC output channel.
type CannotFetchLinksError struct {
	Key cid.Cid
	Err error
}

// Error implements the error interface for this type with a useful
// message.
func (e *CannotFetchLinksError) Error() string {
	return fmt.Sprintf("could not retrieve links for %s: %s", e.Key, e.Err)
}

// CannotDeleteBlockError provides detailed information about which
// blocks could not be deleted and can appear as a Result in the GC output
// channel.
type CannotDeleteBlockError struct {
	Key cid.Cid
	Err error
}

// Error implements the error interface for this type with a
// useful message.
func (e *CannotDeleteBlockError) Error() string {
	return fmt.Sprintf("could not remove %s: %s", e.Key, e.Err)
}
