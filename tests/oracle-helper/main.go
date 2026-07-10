// Command oracle-helper exposes ltx-library operations used by the liters
// interop test suite, pinned to the exact ltx version Litestream v0.5.x uses.
// (The stock `ltx` CLI at v0.5.1 has a broken encode-db that sets Version: 1.)
package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/superfly/ltx"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: oracle-helper <encode-db|decode-db> ...")
	}
	switch args[0] {
	case "encode-db":
		// encode-db OUT DB — encode a SQLite database file as a snapshot LTX
		// (TXID 1, checksum tracking enabled).
		if len(args) != 3 {
			return fmt.Errorf("usage: oracle-helper encode-db OUT DB")
		}
		return encodeDB(args[1], args[2])
	case "decode-db":
		// decode-db OUT LTX — materialize a snapshot LTX as a database file.
		if len(args) != 3 {
			return fmt.Errorf("usage: oracle-helper decode-db OUT LTX")
		}
		return decodeDB(args[1], args[2])
	default:
		return fmt.Errorf("unknown command: %s", args[0])
	}
}

func encodeDB(outPath, dbPath string) error {
	db, err := os.Open(dbPath)
	if err != nil {
		return err
	}
	defer db.Close()

	hdr := make([]byte, 100)
	if _, err := io.ReadFull(db, hdr); err != nil {
		return fmt.Errorf("read db header: %w", err)
	}
	if !bytes.Equal(hdr[:16], []byte("SQLite format 3\x00")) {
		return fmt.Errorf("not a sqlite database")
	}
	pageSize := uint32(binary.BigEndian.Uint16(hdr[16:]))
	if pageSize == 1 {
		pageSize = 65536
	}
	pageN := binary.BigEndian.Uint32(hdr[28:])
	rd := io.MultiReader(bytes.NewReader(hdr), db)

	out, err := os.Create(outPath)
	if err != nil {
		return err
	}
	defer out.Close()

	enc, err := ltx.NewEncoder(out)
	if err != nil {
		return err
	}
	if err := enc.EncodeHeader(ltx.Header{
		Version:   ltx.Version,
		PageSize:  pageSize,
		Commit:    pageN,
		MinTXID:   1,
		MaxTXID:   1,
		Timestamp: time.Now().UnixMilli(),
	}); err != nil {
		return fmt.Errorf("encode header: %w", err)
	}

	var chksum ltx.Checksum
	buf := make([]byte, pageSize)
	for pgno := uint32(1); pgno <= pageN; pgno++ {
		if _, err := io.ReadFull(rd, buf); err != nil {
			return fmt.Errorf("read page %d: %w", pgno, err)
		}
		if pgno == ltx.LockPgno(pageSize) {
			continue
		}
		if err := enc.EncodePage(ltx.PageHeader{Pgno: pgno}, buf); err != nil {
			return fmt.Errorf("encode page %d: %w", pgno, err)
		}
		chksum = ltx.ChecksumFlag | (chksum ^ ltx.ChecksumPage(pgno, buf))
	}

	enc.SetPostApplyChecksum(chksum)
	if err := enc.Close(); err != nil {
		return fmt.Errorf("close encoder: %w", err)
	}
	return out.Sync()
}

func decodeDB(outPath, ltxPath string) error {
	f, err := os.Open(ltxPath)
	if err != nil {
		return err
	}
	defer f.Close()

	out, err := os.Create(outPath)
	if err != nil {
		return err
	}
	defer out.Close()

	dec := ltx.NewDecoder(f)
	if err := dec.DecodeDatabaseTo(out); err != nil {
		return fmt.Errorf("decode database: %w", err)
	}
	return out.Sync()
}
