package main

import (
	"bytes"
	"crypto/rand"
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"time"

	"github.com/jackpal/bencode-go"
)

// bencode_info reflects the "info" dictionary in a .torrent file.
type bencode_info struct {
	Pieces      string `bencode:"pieces"`
	PieceLength int    `bencode:"piece length"`
	Length      int    `bencode:"length"`
	Name        string `bencode:"name"`
}

// bencode_torrent reflects the top-level structure of a .torrent file.
type bencode_torrent struct {
	Announce string       `bencode:"announce"`
	Info     bencode_info `bencode:"info"`
}

// torrent_file is our working representation of a parsed torrent file.
type torrent_file struct {
	announce     string
	info_hash    [20]byte
	piece_hashes [][20]byte
	piece_length int
	length       int
	name         string
}

// open reads and parses a .torrent file from disk.
func open(path string) (torrent_file, error) {
	f, err := os.Open(path)
	if err != nil {
		return torrent_file{}, err
	}
	defer f.Close()

	raw := &bencode_torrent{}
	if err := bencode.Unmarshal(f, raw); err != nil {
		return torrent_file{}, fmt.Errorf("bencode unmarshal error: %w", err)
	}

	return raw.to_torrent_file()
}

func (b *bencode_torrent) to_torrent_file() (torrent_file, error) {
	info_hash, err := b.Info.hash()
	if err != nil {
		return torrent_file{}, err
	}

	piece_hashes, err := b.Info.split_piece_hashes()
	if err != nil {
		return torrent_file{}, err
	}

	return torrent_file{
		announce:     b.Announce,
		info_hash:    info_hash,
		piece_hashes: piece_hashes,
		piece_length: b.Info.PieceLength,
		length:       b.Info.Length,
		name:         b.Info.Name,
	}, nil
}

// hash computes the SHA-1 hash of the bencoded "info" dict.
func (i *bencode_info) hash() ([20]byte, error) {
	var buf bytes.Buffer
	if err := bencode.Marshal(&buf, *i); err != nil {
		return [20]byte{}, err
	}
	return sha1.Sum(buf.Bytes()), nil
}

// split_piece_hashes splits the concatenated 20-byte piece hashes.
func (i *bencode_info) split_piece_hashes() ([][20]byte, error) {
	buf := []byte(i.Pieces)
	if len(buf)%20 != 0 {
		return nil, fmt.Errorf("malformed pieces: length %d is not a multiple of 20", len(buf))
	}

	n := len(buf) / 20
	hashes := make([][20]byte, n)
	for j := 0; j < n; j++ {
		copy(hashes[j][:], buf[j*20:(j+1)*20])
	}
	return hashes, nil
}

// peer represents a single peer connected to a swarm.
type peer struct {
	ip   [4]byte
	port uint16
}

func (p peer) String() string {
	return fmt.Sprintf("%d.%d.%d.%d:%d", p.ip[0], p.ip[1], p.ip[2], p.ip[3], p.port)
}

// unmarshal_peers parses compact peer format (6 bytes per peer: 4 for IP, 2 for Port).
func unmarshal_peers(data []byte) ([]peer, error) {
	if len(data)%6 != 0 {
		return nil, fmt.Errorf("malformed compact peers format: length %d not a multiple of 6", len(data))
	}

	n := len(data) / 6
	peers := make([]peer, n)
	for i := 0; i < n; i++ {
		copy(peers[i].ip[:], data[i*6:i*6+4])
		peers[i].port = binary.BigEndian.Uint16(data[i*6+4 : i*6+6])
	}
	return peers, nil
}

// request_peers contacts the HTTP tracker to get a list of active peers.
func (t *torrent_file) request_peers(peer_id [20]byte, port uint16) ([]peer, error) {
	base, err := url.Parse(t.announce)
	if err != nil {
		return nil, fmt.Errorf("invalid announce URL: %w", err)
	}

	params := url.Values{
		"info_hash":  []string{string(t.info_hash[:])},
		"peer_id":    []string{string(peer_id[:])},
		"port":       []string{strconv.Itoa(int(port))},
		"uploaded":   []string{"0"},
		"downloaded": []string{"0"},
		"compact":    []string{"1"},
		"left":       []string{strconv.Itoa(t.length)},
		"numwant":    []string{"100"},
	}

	base.RawQuery = params.Encode()
	httpClient := &http.Client{Timeout: 15 * time.Second}

	resp, err := httpClient.Get(base.String())
	if err != nil {
		return nil, fmt.Errorf("tracker request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed reading tracker response: %w", err)
	}

	var trackerResp struct {
		Peers string `bencode:"peers"`
	}

	if err := bencode.Unmarshal(bytes.NewReader(body), &trackerResp); err != nil {
		return nil, fmt.Errorf("failed unmarshaling tracker response: %w", err)
	}

	return unmarshal_peers([]byte(trackerResp.Peers))
}

// new_peer_id generates a random 20-byte client identifier.
func new_peer_id() ([20]byte, error) {
	var id [20]byte
	_, err := rand.Read(id[:])
	return id, err
}
