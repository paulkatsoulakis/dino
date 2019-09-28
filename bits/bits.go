package bits

func Put8(b []byte, v uint8) []byte {
	b[0] = v
	return b[1:]
}

func Put16(b []byte, v uint16) []byte {
	b[0] = uint8(v)
	b[1] = uint8(v >> 8)
	return b[2:]
}

func Put32(b []byte, v uint32) []byte {
	b[0] = uint8(v)
	b[1] = uint8(v >> 8)
	b[2] = uint8(v >> 16)
	b[3] = uint8(v >> 24)
	return b[4:]
}

func Put64(b []byte, v uint64) []byte {
	b[0] = uint8(v)
	b[1] = uint8(v >> 8)
	b[2] = uint8(v >> 16)
	b[3] = uint8(v >> 24)
	b[4] = uint8(v >> 32)
	b[5] = uint8(v >> 40)
	b[6] = uint8(v >> 48)
	b[7] = uint8(v >> 56)
	return b[8:]
}

func Putb(b []byte, v []byte) []byte {
	vlen := uint16(len(v))
	b = Put16(b, vlen)
	copy(b, v)
	return b[vlen:]
}

func Puts(b []byte, v string) []byte {
	vlen := uint16(len(v))
	b = Put16(b, vlen)
	copy(b, v)
	return b[vlen:]
}

func Get8(b []byte) (uint8, []byte) {
	return b[0], b[1:]
}

func Get16(b []byte) (uint16, []byte) {
	v := uint16(b[0])
	v += uint16(b[1]) << 8
	return v, b[2:]
}

func Get32(b []byte) (uint32, []byte) {
	v := uint32(b[0])
	v += uint32(b[1]) << 8
	v += uint32(b[2]) << 16
	v += uint32(b[3]) << 24
	return v, b[4:]
}

func Get64(b []byte) (uint64, []byte) {
	v := uint64(b[0])
	v += uint64(b[1]) << 8
	v += uint64(b[2]) << 16
	v += uint64(b[3]) << 24
	v += uint64(b[4]) << 32
	v += uint64(b[5]) << 40
	v += uint64(b[6]) << 48
	v += uint64(b[7]) << 56
	return v, b[8:]
}

func Getb(b []byte) ([]byte, []byte) {
	var vlen uint16
	vlen, b = Get16(b)
	s := make([]byte, vlen)
	copy(s, b[:vlen])
	return s, b[vlen:]
}

func Gets(b []byte) (string, []byte) {
	var vlen uint16
	vlen, b = Get16(b)
	return string(b[:vlen]), b[vlen:]
}
