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
)

// LzmaEncoder compresses the Message bytes using lzma compression. Each
// message is compressed separately.
type LzmaEncoder struct {
}

func (re *LzmaEncoder) Init(config interface{}) (err error) {
	// TODO: allow config to specify the compression preset.
	return
}

func (re *LzmaEncoder) Encode(pack *PipelinePack) (output []byte, err error) {
	var b bytes.Buffer
	bw := bufio.NewWriter(&b)

	// TODO: configurable int level to replace "lzma.BestSpeed" here.
	w := lzma.NewWriterSizeLevel(bw, int64(len(pack.MsgBytes)), lzma.BestSpeed)
	w.Write(pack.MsgBytes)
	w.Close()
	bw.Flush()

	output = b.Bytes()
	return
}

func init() {
	RegisterPlugin("LzmaEncoder", func() interface{} {
		return new(LzmaEncoder)
	})
}
