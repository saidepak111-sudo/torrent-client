package main

import (
	"fmt"
	"log"
	"net"
	"time"
)

// client maintains state for a TCP connection to a single peer.
type client struct {
	conn      net.Conn
	peer      peer
	info_hash [20]byte
	peer_id   [20]byte
	bitfield  bitfield
	choked    bool
}

// new_client dials a peer, completes handshake, and reads the peer bitfield.
func new_client(p peer, info_hash [20]byte, peer_id [20]byte, num_pieces int) (*client, error) {
	conn, err := net.DialTimeout("tcp", p.String(), 10*time.Second)
	if err != nil {
		return nil, err
	}

	log.Printf("connected to peer %s, initiating protocol handshake...\n", p)

	if err := do_handshake(conn, info_hash, peer_id); err != nil {
		conn.Close()
		return nil, fmt.Errorf("handshake failed with %s: %w", p, err)
	}

	bf, err := recv_bitfield(conn, num_pieces)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed receiving bitfield from %s: %w", p, err)
	}

	return &client{
		conn:      conn,
		peer:      p,
		info_hash: info_hash,
		peer_id:   peer_id,
		bitfield:  bf,
		choked:    true,
	}, nil
}

func do_handshake(conn net.Conn, info_hash [20]byte, peer_id [20]byte) error {
	conn.SetDeadline(time.Now().Add(5 * time.Second))
	defer conn.SetDeadline(time.Time{})

	h := new_handshake(info_hash, peer_id)
	if _, err := conn.Write(h.serialize()); err != nil {
		return err
	}

	received, err := read_handshake(conn)
	if err != nil {
		return err
	}

	if received.info_hash != info_hash {
		return fmt.Errorf("infohash mismatch: expected %x, got %x", info_hash, received.info_hash)
	}

	return nil
}

func recv_bitfield(conn net.Conn, num_pieces int) (bitfield, error) {
	conn.SetDeadline(time.Now().Add(5 * time.Second))
	defer conn.SetDeadline(time.Time{})

	msg, err := read_message(conn)
	if err != nil {
		// If peer doesn't send bitfield, return empty bitfield (we'll collect piece availability via 'have' messages)
		return make(bitfield, (num_pieces+7)/8), nil
	}
	if msg == nil || msg.id != msg_bitfield {
		return make(bitfield, (num_pieces+7)/8), nil
	}
	return msg.payload, nil
}

func (c *client) send_request(index, begin, length int) error {
	msg := format_request(index, begin, length)
	_, err := c.conn.Write(msg.serialize())
	return err
}

func (c *client) send_interested() error {
	msg := &message{id: msg_interested}
	_, err := c.conn.Write(msg.serialize())
	return err
}

func (c *client) send_not_interested() error {
	msg := &message{id: msg_not_interested}
	_, err := c.conn.Write(msg.serialize())
	return err
}

func (c *client) send_unchoke() error {
	msg := &message{id: msg_unchoke}
	_, err := c.conn.Write(msg.serialize())
	return err
}

func (c *client) send_have(index int) error {
	msg := format_have(index)
	_, err := c.conn.Write(msg.serialize())
	return err
}

func (c *client) read() (*message, error) {
	return read_message(c.conn)
}
