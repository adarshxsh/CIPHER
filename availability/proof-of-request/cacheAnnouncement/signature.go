package cacheAnnouncement

import (
	"bytes"
	"crypto/ed25519"
	"encoding/binary"
	"errors"

	"proof-of-request/model"
)

func buildSigningPayload(
	announcement model.CacheAnnouncement,
) ([]byte, error) {
	if announcement.ProviderID == "" {
		return nil, errors.New("provider ID cannot be empty")
	}

	if announcement.FileID == "" {
		return nil, errors.New("file ID cannot be empty")
	}

	if announcement.MerkleRoot == "" {
		return nil, errors.New("Merkle root cannot be empty")
	}

	var buf bytes.Buffer

	// ProviderID
	if err := writeString(&buf, announcement.ProviderID); err != nil {
		return nil, err
	}

	// FileID
	if err := writeString(&buf, announcement.FileID); err != nil {
		return nil, err
	}

	// MerkleRoot
	if err := writeString(&buf, announcement.MerkleRoot); err != nil {
		return nil, err
	}

	// Expiry
	if err := binary.Write(
		&buf,
		binary.BigEndian,
		announcement.Expiry.UnixNano(),
	); err != nil {
		return nil, err
	}

	// Version
	if err := binary.Write(
		&buf,
		binary.BigEndian,
		announcement.Version,
	); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func writeString(buf *bytes.Buffer, value string) error {
	if err := binary.Write(
		buf,
		binary.BigEndian,
		uint64(len(value)),
	); err != nil {
		return err
	}

	_, err := buf.WriteString(value)
	return err
}

func SignCacheAnnouncement(
	announcement model.CacheAnnouncement,
	privateKey ed25519.PrivateKey,
) (model.SignedCacheAnnouncement, error) {

	payload, err := buildSigningPayload(announcement)
	if err != nil {
		return model.SignedCacheAnnouncement{}, err
	}

	signature := ed25519.Sign(privateKey, payload)

	return model.SignedCacheAnnouncement{
		Announcement: announcement,
		Signature:    signature,
	}, nil
}

func VerifyCacheAnnouncement(
	signedAnnouncement model.SignedCacheAnnouncement,
	publicKey ed25519.PublicKey,
) bool {
	payload, err := buildSigningPayload(signedAnnouncement.Announcement)
	if err != nil {
		return false
	}

	return ed25519.Verify(
		publicKey,
		payload,
		signedAnnouncement.Signature,
	)
}
