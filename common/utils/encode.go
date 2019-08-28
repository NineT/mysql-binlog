package utils

import (
	"errors"
	"math"
	"reflect"
	"unsafe"
)

/**
typedef struct my_base64_decoder_t
{
const char *src; /* Pointer to the current input position
const char *end; /* Pointer to the end of input buffer
uint c;          /* Collect bits into this number
int error;       /* Error code
uchar state;     /* Character number in the current group of 4
uchar mark;      /* Number of padding marks in the current group
} MY_BASE64_DECODER;
**/

type base64Decoder struct {
	src   []byte // Pointer to the current input position
	end   []byte
	c     uint  // Collect bits into this number
	err   int   // Error code
	state uint8 // Character number in the current group of 4
	mark  uint8 // Number of padding marks in the current group
}

const (
	//MY_BASE64_DECODE_ALLOW_MULTIPLE_CHUNKS 1
	MyBase64DecodeAllowMultipleChunks = 1
)

/**
according to mysql-5.6.39/mysys/base64.c
*/

// base64 table from mysql-5.6.39/mysys/base64.c base64Table[]
var (
	base64Table = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"

	fromBase64Table = []int8{
		-1, -1, -1, -1, -1, -1, -1, -1, -1, -2, -2, -2, -2, -2, -1, -1,
		-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
		-2, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 62, -1, -1, -1, 63,
		52, 53, 54, 55, 56, 57, 58, 59, 60, 61, -1, -1, -1, -1, -1, -1,
		-1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14,
		15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, -1, -1, -1, -1, -1,
		-1, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40,
		41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, -1, -1, -1, -1, -1,
		-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
		-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
		-2, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
		-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
		-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
		-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
		-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
		-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
	}
)

func (decoder *base64Decoder) Add(num int) {
	p := (*reflect.SliceHeader)(unsafe.Pointer(&decoder.src))
	p.Data = p.Data + uintptr(num) // 长度不变
}

// base64EncodedLength  [51L] base64_needed_encoded_length(uint64 length_of_data)
func base64EncodedLength(dataLen uint64) uint64 {
	if dataLen == 0 {
		return 1
	}

	length := (dataLen + 2) / 3 * 4

	/* base64 char incl padding */
	/* newlines */
	/* NUL termination of string */
	return length + (length-1)/76
}

/**
uint64
base64_needed_decoded_length(uint64 length_of_encoded_data)
{
  return (uint64) ceil(length_of_encoded_data * 3 / 4);
}
*/
//
func base64DecodeLength(len uint64) uint64 {
	return uint64(math.Ceil(float64(len) * 3 / 4))
}

// Base64Encode according to [94L]base64_encode(const void *src, size_t src_len, char *dst)
func Base64Encode(src []byte) []byte {
	srcLen := len(src)
	dst := make([]byte, base64EncodedLength(uint64(srcLen)))

	dstIdx := 0
	idx := 0
	for i := 0; i < srcLen; idx += 4 {
		if idx == 76 {
			idx = 0
			dst[dstIdx] = '\n'
			dstIdx++
		}

		c := uint32(src[i])
		i++

		c = c << 8

		if i < srcLen {
			c = c + uint32(src[i])
		}
		c = c << 8
		i++

		if i < srcLen {
			c = c + uint32(src[i])
		}
		i++

		dst[dstIdx] = base64Table[(c>>18)&0x3f]
		dstIdx++

		dst[dstIdx] = base64Table[(c>>12)&0x3f]
		dstIdx++

		if i > (srcLen + 1) {
			dst[dstIdx] = '='
			dstIdx++
		} else {
			dst[dstIdx] = base64Table[(c>>6)&0x3f]
			dstIdx++
		}

		if i > srcLen {
			dst[dstIdx] = '='
			dstIdx++
		} else {
			dst[dstIdx] = base64Table[(c>>0)&0x3f]
			dstIdx++
		}
	}

	return dst
}

/**
### dst_len= base64_decode(str, strlen(str), dst, &end_ptr, 0);

int64
base64_decode(const char *src_base, size_t len,
              void *dst, const char **end_ptr, int flags)
{
  char *d= (char*) dst;
  MY_BASE64_DECODER base64Decoder;

  base64Decoder.src= src_base;
  base64Decoder.end= src_base + len;
  base64Decoder.error= 0;
  base64Decoder.mark= 0;

  for ( ; ; )
  {
    base64Decoder.c= 0;
    base64Decoder.state= 0;

    if (my_base64_decoder_getch(&base64Decoder) ||
        my_base64_decoder_getch(&base64Decoder) ||
        my_base64_decoder_getch(&base64Decoder) ||
        my_base64_decoder_getch(&base64Decoder))
      break;

    *d++= (base64Decoder.c >> 16) & 0xff;
    *d++= (base64Decoder.c >>  8) & 0xff;
    *d++= (base64Decoder.c >>  0) & 0xff;

    if (base64Decoder.mark)
    {
      d-= base64Decoder.mark;
      if (!(flags & MY_BASE64_DECODE_ALLOW_MULTIPLE_CHUNKS))
        break;
      base64Decoder.mark= 0;
    }
  }

  Return error if there are more non-space characters
base64Decoder.state= 0;
if (!my_base64_decoder_skip_spaces(&base64Decoder))
base64Decoder.error= 1;

if (end_ptr != NULL)
*end_ptr= base64Decoder.src;

return base64Decoder.error ? -1 : (int) (d - (char*) dst);
}
*/

// decode
func Base64Decode(src []byte, flag int) ([]byte, error) {
	size := len(src)
	dst := make([]byte, base64DecodeLength(uint64(size)))

	decoder := &base64Decoder{
		src:  src[0:1],
		end:  src[size-1 : size],
		err:  0,
		mark: 0,
	}

	i := 0
	for {
		decoder.c = 0
		decoder.state = 0

		if getch(decoder) || getch(decoder) || getch(decoder) || getch(decoder) {
			break
		}

		dst[i] = byte((decoder.c >> 16) & 0xff)
		i++
		dst[i] = byte((decoder.c >> 8) & 0xff)
		i++
		dst[i] = byte((decoder.c >> 0) & 0xff)
		i++

		if decoder.mark != 0 {
			dst[i] = dst[i] - decoder.mark

			if (flag & MyBase64DecodeAllowMultipleChunks) == 0 {
				break
			}

			decoder.mark = 0
		}
	}

	decoder.state = 0

	if !skipSpace(decoder) {
		decoder.err = 1
	}

	if decoder.err == 1 {
		return nil, errors.New("decode error")
	}

	return dst[0:i], nil
}

/**
my_base64_decoder_skip_spaces(MY_BASE64_DECODER *decoder)
{
  for ( ; decoder->src < decoder->end; decoder->src++)
  {
    if (from_base64_table[(uchar) *decoder->src] != -2)
      return FALSE;
  }
  if (decoder->state > 0)
    decoder->error= 1; /* Unexpected end-of-input found
return TRUE;
}
*/
func skipSpace(decoder *base64Decoder) bool {
	end := (*reflect.SliceHeader)(unsafe.Pointer(&decoder.end))
	start := (*reflect.SliceHeader)(unsafe.Pointer(&decoder.src))

	for ; start.Data <= end.Data; start.Data++ {

		if fromBase64Table[decoder.src[0]] != -2 {
			return false
		}
	}

	if decoder.state > 0 {
		decoder.err -= 1
	}
	return true
}

/***
my_base64_add(MY_BASE64_DECODER *decoder)
{
  int res;
  decoder->c <<= 6;
  if ((res= from_base64_table[(uchar) *decoder->src++]) < 0)
    return (decoder->error= TRUE);
  decoder->c+= (uint) res;
  return FALSE;
}
*/
func add(decoder *base64Decoder) bool {
	defer decoder.Add(1)
	decoder.c = decoder.c << 6

	res := fromBase64Table[decoder.src[0]]
	if res < 0 {
		return decoder.err == 1
	}

	decoder.c += uint(res)

	return false
}

/***
 */
func getch(decoder *base64Decoder) bool {
	if skipSpace(decoder) {
		return true
	}

	if !add(decoder) {
		if decoder.mark != 0 {
			decoder.err = 1
			decoder.Add(-1)
			return true
		}

		decoder.state++
		return false
	}

	switch decoder.state {
	case 0, 1:
		decoder.Add(-1)
		return true
	case 2, 3:
		b := decoder.src
		p := (*reflect.SliceHeader)(unsafe.Pointer(&b))
		p.Data = p.Data - 1 // 长度不变
		if b[0] == '=' {
			decoder.err = 0
			decoder.mark++
		} else {
			decoder.Add(-1)
			return true
		}
	default:
		return true

	}

	decoder.state++
	return false
}

// AboutEncodedSize
func AboutEncodedSize(s int) int {
	return (s << 1) - (s >> 1)
}