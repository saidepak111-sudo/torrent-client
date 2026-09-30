package main

import (
	"bytes"
	"encoding/binary"
	"net"
	"testing"
)

func TestBitfield(t *testing.T) {
	bf := make(bitfield, 2) // 16 bits capacity

	// Piece 0 -> Byte 0, Bit 7 (MSB)
	if bf.has_piece(0) {
		t.Errorf("expected bitfield to not have piece 0")
	}

	bf.set_piece(0)
	if !bf.has_piece(0) {
		t.Errorf("expected bitfield to have piece 0")
	}

	// Verify exact byte value (0b10000000 = 0x80)
	if bf[0] != 0x80 {
		t.Errorf("expected byte 0 to be 0x80, got 0x%x", bf[0])
	}

	// Piece 7 -> Byte 0, Bit 0 (LSB)
	bf.set_piece(7)
	if bf[0] != 0x81 { // 0x80 | 0x01 = 0x81
		t.Errorf("expected byte 0 to be 0x81, got 0x%x", bf[0])
	}

	// Piece 10 -> Byte 1, Bit 5 (MSB offset 2 -> 1 << (7 - 2) = 0x20)
	bf.set_piece(10)
	if !bf.has_piece(10) {
		t.Errorf("expected bitfield to have piece 10")
	}
	if bf[1] != 0x20 {
		t.Errorf("expected byte 1 to be 0x20, got 0x%x", bf[1])
	}
}

func TestFormatRequest(t *testing.T) {
	msg := format_request(4, 16384, 16384)
	if msg.id != msg_request {
		t.Errorf("expected message id %d, got %d", msg_request, msg.id)
	}

	serialized := msg.serialize()
	// Total length: 4 (len) + 1 (id) + 12 (payload) = 17 bytes
	if len(serialized) != 17 {
		t.Errorf("expected serialized length 17, got %d", len(serialized))
	}

	msgLen := binary.BigEndian.Uint32(serialized[0:4])
	if msgLen != 13 {
		t.Errorf("expected msg length field to be 13, got %d", msgLen)
	}

	index := binary.BigEndian.Uint32(serialized[5:9])
	begin := binary.BigEndian.Uint32(serialized[9:13])
	length := binary.BigEndian.Uint32(serialized[13:17])

	if index != 4 || begin != 16384 || length != 16384 {
		t.Errorf("mismatch in request fields: index=%d, begin=%d, length=%d", index, begin, length)
	}
}

func TestHandshakeSerializeRead(t *testing.T) {
	var infoHash, peerID [20]byte
	copy(infoHash[:], "12345678901234567890")
	copy(peerID[:], "abcdefghijklmnopqrst")

	h := new_handshake(infoHash, peerID)
	serialized := h.serialize()

	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	go func() {
		clientConn.Write(serialized)
	}()

	parsed, err := read_handshake(serverConn)
	if err != nil {
		t.Fatalf("read_handshake failed: %v", err)
	}

	if parsed.pstr != "BitTorrent protocol" {
		t.Errorf("unexpected pstr: %s", parsed.pstr)
	}
	if !bytes.Equal(parsed.info_hash[:], infoHash[:]) {
		t.Errorf("infohash mismatch")
	}
	if !bytes.Equal(parsed.peer_id[:], peerID[:]) {
		t.Errorf("peer_id mismatch")
	}
}
