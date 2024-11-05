
// Copyright (C) 2016 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

//go:build !(solaris && !cgo) && !(darwin && !cgo) && !(darwin && kqueue) && !(android && amd64) && !ios
// +build !solaris cgo
// +build !darwin cgo
// +build !darwin !kqueue
// +build !android !amd64
// +build !ios

=======

package fs

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"unicode/utf8"
	"github.com/sjansen/watchman" // Import Watchman client library
)

// Notify does not block on sending to channel, so the channel must be buffered.
var backendBuffer = 500

func (f *BasicFilesystem) Watch(name string, ignore Matcher, ctx context.Context, ignorePerms bool) (<-chan Event, <-chan error, error) {
	watchPath, _, err := f.watchPaths(name)
	if err != nil {
		return nil, nil, err
	}

	// Clean up the watch path
	watchPath = filepath.Clean(watchPath)

	// Prepare a separate variable for the AddWatch call without the "/..." suffix
	addWatchPath := strings.TrimSuffix(watchPath, "/...")

	outChan := make(chan Event, backendBuffer)
	errChan := make(chan error, 100)

	// Initialize the Watchman client
	wClient, err := watchman.Connect()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to connect to Watchman: %w", err)
	}

	// Ensure the client connection is closed when done
	go func() {
		<-ctx.Done()
		wClient.Close()
	}()

	// Set up a watch on the directory without "/..."
	watch, err := wClient.AddWatch(addWatchPath)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to set up watch for directory: %w", err)
	}

	// Subscribe to changes with a unique subscription name
	sub, err := watch.Subscribe("sub-"+name, addWatchPath)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to subscribe to directory events: %w", err)
	}

	// Start the watch loop
	go f.watchLoop(ctx, watchPath, wClient.Notifications(), outChan, errChan, ignore)

	// Unsubscribe when context is done
	go func() {
		<-ctx.Done()
		if err := sub.Unsubscribe(); err != nil {
			errChan <- fmt.Errorf("failed to unsubscribe: %w", err)
		}
		wClient.Close()
	}()

	return outChan, errChan, nil
}

func (f *BasicFilesystem) watchLoop(ctx context.Context, rootPath string, notifications <-chan interface{}, outChan chan<- Event, errChan chan<- error, ignore Matcher) {
	defer close(outChan)
	defer close(errChan)

	for {
		select {
		case n := <-notifications:
			changeNotif, ok := n.(*watchman.ChangeNotification)
			if !ok || changeNotif.IsFreshInstance {
				continue
			}

			// Process each file event in the change notification
			for _, file := range changeNotif.Files {
				// Create absolute path for use with unrootedChecked
				absPath := filepath.Join(rootPath, file.Name)
				
				if !utf8.ValidString(absPath) {
					l.Debugln(f.Type(), f.URI(), "Watch: Ignoring invalid UTF-8")
					continue
				}

				// Pass absolute path to unrootedChecked for validation
				relPath, err := f.unrootedChecked(absPath, []string{rootPath})
				if err != nil {
					l.Debugln(f.Type(), f.URI(), "Watch: Event outside root path:", absPath)
					continue
				}

				if ignore.Match(relPath).IsIgnored() {
					l.Debugln(f.Type(), f.URI(), "Watch: Ignoring", relPath)
					continue
				}

				evType := f.mapWatchmanEventType(file.Change)
				select {
				case outChan <- Event{Name: file.Name, Type: evType}: // Send only the relative path
					l.Debugln(f.Type(), f.URI(), "Watch: Sending", file.Name, evType)
				case <-ctx.Done():
					l.Debugln(f.Type(), f.URI(), "Watch: Stopped")
					return
				}
			}
		case <-ctx.Done():
			l.Debugln(f.Type(), f.URI(), "Watch: Stopped")
			return
		}
	}
}

// Map Watchman event types to custom EventType
func (f *BasicFilesystem) mapWatchmanEventType(change watchman.StateChange) EventType {
	switch change {
	case watchman.Removed:
		return Remove
	case watchman.Created:
		return NonRemove
	case watchman.Updated:
		return NonRemove
	default:
		return NonRemove
	}
}
