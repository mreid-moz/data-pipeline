/***** BEGIN LICENSE BLOCK *****
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
# ***** END LICENSE BLOCK *****/

package lz

import (
	"bufio"
	"bytes"
	"code.google.com/p/lzma"
	. "github.com/mozilla-services/heka/pipeline"
	"io"
)

// LzmaDecoder decompresses lzma-compressed Message bytes.
type LzmaDecoder struct {
}

func (re *LzmaDecoder) Init(config interface{}) (err error) {
	return
}

func (re *LzmaDecoder) Decode(pack *PipelinePack) (packs []*PipelinePack, err error) {
	var decompressed bytes.Buffer
	dw := bufio.NewWriter(&decompressed)

	b := bytes.NewBuffer(pack.MsgBytes)
	r := lzma.NewReader(b)
	defer r.Close()
	// the os.Stdout would be an io.Writer used to write uncompressed data to
	_, decodeErr := io.Copy(dw, r)
	dw.Flush()
	packs = []*PipelinePack{pack}
	if decodeErr == nil {
		// Replace bytes with decoded data
		pack.MsgBytes = decompressed.Bytes()
	}
	// If there is an error decompressing, maybe it wasn't compressed. We'll
	// return the original data and try to proceed.
	return
}

func init() {
	RegisterPlugin("LzmaDecoder", func() interface{} {
		return new(LzmaDecoder)
	})
}
