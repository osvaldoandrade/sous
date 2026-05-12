package wasm

// This file is a test-only mini-builder for WebAssembly binaries. It
// is the cheapest possible way to give the cs-wasm runner real wasm
// inputs without dragging a full WAT compiler into the dependency
// graph. The encoder is deliberately incomplete — it only implements
// the sections the tests use — but every byte it produces lives at
// well-defined offsets per the wasm core spec:
//
//	https://webassembly.github.io/spec/core/binary/modules.html

import "bytes"

const (
	wasmMagic   = "\x00asm"
	wasmVersion = "\x01\x00\x00\x00"

	sectionType   = 0x01
	sectionImport = 0x02
	sectionFunc   = 0x03
	sectionMem    = 0x05
	sectionExport = 0x07
	sectionCode   = 0x0A
	sectionData   = 0x0B

	valI32 = 0x7F
	valI64 = 0x7E

	importFunc  = 0x00
	exportFunc  = 0x00
	exportMem   = 0x02
	opEnd       = 0x0B
	opCall      = 0x10
	opI32Const  = 0x41
	opI64Const  = 0x42
	opI64Or     = 0x84
	opI64Shl    = 0x86
	opI64Extend = 0xAD // i64.extend_i32_u
)

// wasmBuilder assembles a minimal wasm module out of typed sections.
// The zero value is ready to use.
type wasmBuilder struct {
	types   []funcType
	imports []importEntry
	funcs   []uint32 // type indices for each local function
	exports []exportEntry
	codes   [][]byte
	memMin  uint32 // initial page count, 0 means no memory section
	dataBuf []byte // optional initial data segment for memory[0] @ offset 0
}

type funcType struct {
	params  []byte
	results []byte
}

type importEntry struct {
	module, name string
	typeIdx      uint32
}

type exportEntry struct {
	name string
	kind byte
	idx  uint32
}

func uleb128(v uint32) []byte {
	var out []byte
	for {
		b := byte(v & 0x7F)
		v >>= 7
		if v != 0 {
			b |= 0x80
			out = append(out, b)
			continue
		}
		out = append(out, b)
		return out
	}
}

func sleb128i32(v int32) []byte {
	var out []byte
	more := true
	for more {
		b := byte(v & 0x7F)
		v >>= 7
		sign := b & 0x40
		if (v == 0 && sign == 0) || (v == -1 && sign != 0) {
			more = false
		} else {
			b |= 0x80
		}
		out = append(out, b)
	}
	return out
}

func encodeName(s string) []byte {
	out := uleb128(uint32(len(s)))
	return append(out, s...)
}

func encodeFuncType(t funcType) []byte {
	out := []byte{0x60}
	out = append(out, uleb128(uint32(len(t.params)))...)
	out = append(out, t.params...)
	out = append(out, uleb128(uint32(len(t.results)))...)
	out = append(out, t.results...)
	return out
}

func (b *wasmBuilder) addType(params, results []byte) uint32 {
	idx := uint32(len(b.types))
	b.types = append(b.types, funcType{params: params, results: results})
	return idx
}

func (b *wasmBuilder) addImport(module, name string, typeIdx uint32) uint32 {
	idx := uint32(len(b.imports))
	b.imports = append(b.imports, importEntry{module: module, name: name, typeIdx: typeIdx})
	return idx
}

func (b *wasmBuilder) addFunc(typeIdx uint32, body []byte) uint32 {
	idx := uint32(len(b.imports)) + uint32(len(b.funcs))
	b.funcs = append(b.funcs, typeIdx)
	b.codes = append(b.codes, body)
	return idx
}

func (b *wasmBuilder) addExport(name string, kind byte, idx uint32) {
	b.exports = append(b.exports, exportEntry{name: name, kind: kind, idx: idx})
}

func (b *wasmBuilder) build() []byte {
	var out bytes.Buffer
	out.WriteString(wasmMagic)
	out.WriteString(wasmVersion)

	if len(b.types) > 0 {
		var s bytes.Buffer
		s.Write(uleb128(uint32(len(b.types))))
		for _, t := range b.types {
			s.Write(encodeFuncType(t))
		}
		writeSection(&out, sectionType, s.Bytes())
	}

	if len(b.imports) > 0 {
		var s bytes.Buffer
		s.Write(uleb128(uint32(len(b.imports))))
		for _, imp := range b.imports {
			s.Write(encodeName(imp.module))
			s.Write(encodeName(imp.name))
			s.WriteByte(importFunc)
			s.Write(uleb128(imp.typeIdx))
		}
		writeSection(&out, sectionImport, s.Bytes())
	}

	if len(b.funcs) > 0 {
		var s bytes.Buffer
		s.Write(uleb128(uint32(len(b.funcs))))
		for _, t := range b.funcs {
			s.Write(uleb128(t))
		}
		writeSection(&out, sectionFunc, s.Bytes())
	}

	if b.memMin > 0 {
		var s bytes.Buffer
		s.WriteByte(0x01)          // one memory
		s.WriteByte(0x00)          // limit: no max
		s.Write(uleb128(b.memMin)) // initial pages
		writeSection(&out, sectionMem, s.Bytes())
	}

	if len(b.exports) > 0 {
		var s bytes.Buffer
		s.Write(uleb128(uint32(len(b.exports))))
		for _, e := range b.exports {
			s.Write(encodeName(e.name))
			s.WriteByte(e.kind)
			s.Write(uleb128(e.idx))
		}
		writeSection(&out, sectionExport, s.Bytes())
	}

	if len(b.codes) > 0 {
		var s bytes.Buffer
		s.Write(uleb128(uint32(len(b.codes))))
		for _, body := range b.codes {
			var fn bytes.Buffer
			fn.WriteByte(0x00) // no local declarations
			fn.Write(body)
			fn.WriteByte(opEnd)
			s.Write(uleb128(uint32(fn.Len())))
			s.Write(fn.Bytes())
		}
		writeSection(&out, sectionCode, s.Bytes())
	}

	if len(b.dataBuf) > 0 {
		var s bytes.Buffer
		s.Write(uleb128(1)) // one data segment
		s.WriteByte(0x00)   // active, memory 0
		s.WriteByte(opI32Const)
		s.Write(sleb128i32(0)) // offset = 0
		s.WriteByte(opEnd)
		s.Write(uleb128(uint32(len(b.dataBuf))))
		s.Write(b.dataBuf)
		writeSection(&out, sectionData, s.Bytes())
	}

	return out.Bytes()
}

func writeSection(out *bytes.Buffer, id byte, body []byte) {
	out.WriteByte(id)
	out.Write(uleb128(uint32(len(body))))
	out.Write(body)
}
