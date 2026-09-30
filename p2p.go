package main

import (
	"bytes"
	"crypto/sha1"
	"fmt"
	"log"
	"runtime"
	"time"
)

const block_size = 16384 // Standard 16 KB block size
const max_backlog = 5    // Maximum pipeline depth (requests in flight)

type piece_work struct {
	index  int
	hash   [20]byte
	length int
}

type piece_result struct {
	index int
	data  []byte
}

type piece_progress struct {
	index      int
	buf        []byte
	downloaded int
	requested  int
	backlog    int
}

// fill_requests pipelines block requests to the peer up to max_backlog.
func (pp *piece_progress) fill_requests(c *client, piece_length int) error {
	for pp.backlog < max_backlog && pp.requested < piece_length {
		block_len := block_size
		if piece_length-pp.requested < block_size {
			block_len = piece_length - pp.requested
		}
		if err := c.send_request(pp.index, pp.requested, block_len); err != nil {
			return err
		}
		pp.backlog++
		pp.requested += block_len
	}
	return nil
}

// handle_message processes incoming wire messages and updates piece progress.
func (pp *piece_progress) handle_message(c *client) error {
	msg, err := c.read()
	if err != nil {
		return err
	}
	if msg == nil {
		return nil // Keep-alive message
	}

	switch msg.id {
	case msg_unchoke:
		c.choked = false
		log.Printf("peer %s unchoked us\n", c.peer)

	case msg_choke:
		c.choked = true

	case msg_have:
		index, err := parse_have(msg)
		if err != nil {
			return err
		}
		c.bitfield.set_piece(index)

	case msg_piece:
		n, err := parse_piece(pp.index, pp.buf, msg)
		if err != nil {
			return err
		}
		pp.downloaded += n
		pp.backlog-- // Decrement backlog so fill_requests can pipeline the next blocks!
	}

	return nil
}

// download_piece manages block pipelining to download a full piece from a peer.
func download_piece(c *client, pw *piece_work) ([]byte, error) {
	pp := &piece_progress{
		index: pw.index,
		buf:   make([]byte, pw.length),
	}

	c.conn.SetDeadline(time.Now().Add(30 * time.Second))
	defer c.conn.SetDeadline(time.Time{})

	for pp.downloaded < pw.length {
		if !c.choked {
			if err := pp.fill_requests(c, pw.length); err != nil {
				return nil, err
			}
		}
		if err := pp.handle_message(c); err != nil {
			return nil, err
		}
	}

	return pp.buf, nil
}

// check_integrity verifies that downloaded piece data matches the expected SHA-1 hash.
func check_integrity(pw *piece_work, data []byte) error {
	hash := sha1.Sum(data)
	if !bytes.Equal(hash[:], pw.hash[:]) {
		return fmt.Errorf("piece %d failed integrity check (hash mismatch)", pw.index)
	}
	return nil
}

// start_download_worker runs in a goroutine for a connected peer, pulling work from work_ch.
func start_download_worker(p peer, info_hash [20]byte, peer_id [20]byte, work_ch chan *piece_work, results_ch chan *piece_result, num_pieces int) {
	c, err := new_client(p, info_hash, peer_id, num_pieces)
	if err != nil {
		log.Printf("could not connect to peer %s: %v\n", p, err)
		return
	}
	defer c.conn.Close()

	if err := c.send_unchoke(); err != nil {
		return
	}
	if err := c.send_interested(); err != nil {
		return
	}

	for pw := range work_ch {
		if !c.bitfield.has_piece(pw.index) {
			// Re-queue work and yield briefly to prevent spinning
			work_ch <- pw
			time.Sleep(100 * time.Millisecond)
			continue
		}

		data, err := download_piece(c, pw)
		if err != nil {
			work_ch <- pw
			log.Printf("failed downloading piece %d from %s: %v\n", pw.index, p, err)
			return
		}

		if err := check_integrity(pw, data); err != nil {
			work_ch <- pw
			log.Printf("piece %d from %s failed SHA-1 integrity check: %v\n", pw.index, p, err)
			continue
		}

		c.send_have(pw.index)
		results_ch <- &piece_result{pw.index, data}
	}
}

// download orchestrates the worker pool to download all pieces of the torrent.
func (t *torrent_file) download() ([]byte, error) {
	log.Println("starting download for torrent:", t.name)

	peer_id, err := new_peer_id()
	if err != nil {
		return nil, fmt.Errorf("failed generating peer id: %w", err)
	}

	peers, err := t.request_peers(peer_id, 6881)
	if err != nil {
		return nil, fmt.Errorf("failed fetching peers from tracker: %w", err)
	}
	log.Printf("retrieved %d peers from tracker\n", len(peers))

	work_ch := make(chan *piece_work, len(t.piece_hashes))
	results_ch := make(chan *piece_result)

	for index, hash := range t.piece_hashes {
		length := t.piece_length_at(index)
		work_ch <- &piece_work{index, hash, length}
	}

	for _, p := range peers {
		go start_download_worker(p, t.info_hash, peer_id, work_ch, results_ch, len(t.piece_hashes))
	}

	buf := make([]byte, t.length)
	done_pieces := 0

	for done_pieces < len(t.piece_hashes) {
		result := <-results_ch

		begin := result.index * t.piece_length
		end := begin + len(result.data)
		copy(buf[begin:end], result.data)
		done_pieces++

		percent := float64(done_pieces) / float64(len(t.piece_hashes)) * 100
		num_workers := runtime.NumGoroutine() - 1
		log.Printf("%.2f%% done — piece %d downloaded (%d active workers)\n", percent, result.index, num_workers)
	}
	close(work_ch)

	return buf, nil
}

func (t *torrent_file) piece_length_at(index int) int {
	begin := index * t.piece_length
	end := begin + t.piece_length
	if end > t.length {
		end = t.length
	}
	return end - begin
}
