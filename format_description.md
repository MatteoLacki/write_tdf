```markdown
# TIMS Frame Binary Format Description

This document describes the binary layout of TIMS frame data as illustrated in the diagram.

## Overall Layout

The data consists of multiple **TIMS frames stored sequentially** inside a binary blob. Each frame contains:

1. A fixed-size **header**
2. A **compressed payload** containing the frame data

Frames are stored **back-to-back**, possibly separated by padding bytes.

```

[Frame 1][padding][Frame 2][padding][Frame 3] ...

```

Each frame begins at the offset referenced by `Frames.TimsId`.

---

# Frame Structure

Each **TIMS frame** consists of two components:

1. **Header (8 bytes, uncompressed)**
2. **Compressed frame data (variable length)**

---

# Header

The header is **8 bytes long** and contains **two 32-bit unsigned integers (`uint32_t`)**.

| Field | Type | Size | Description |
|------|------|------|-------------|
| `bin_size` | uint32_t | 4 bytes | Total byte length of the binary frame block (header + compressed frame data) |
| `num_scans` | uint32_t | 4 bytes | Number of TOF scans contained in the TIMS frame |

Header layout:

```

uint32_t bin_size
uint32_t num_scans

```

### Meaning of `bin_size`

`bin_size` represents the **total size of the frame block**, including:

- the header
- the compressed frame data

Therefore:

```

bin_size = header_size + compressed_frame_size

```

Since the header is always **8 bytes**, the compressed payload length is:

```

compressed_size = bin_size - 8

```

---

# Compressed Frame Data

Immediately following the header is a **byte array containing the compressed frame payload**.

```

uint8_t compressed_frame_data[compressed_size]

```

The length of this array is determined using the `bin_size` field from the header.

This compressed payload contains the **encoded scan data for the frame**.

---

# Padding Between Frames

After the compressed payload, there may be **padding bytes** before the next frame begins.

Padding may be present for reasons such as:

- alignment requirements
- storage layout constraints
- database block alignment

The next frame begins at the offset referenced by the next `Frames.TimsId`.

---

# Parsing a Frame

To parse a frame from the binary blob:

1. Read **8 bytes** to obtain the header.
2. Interpret the first 4 bytes as `bin_size`.
3. Interpret the next 4 bytes as `num_scans`.
4. Compute the compressed payload size:

```

compressed_size = bin_size - 8

```

5. Read `compressed_size` bytes immediately following the header.
6. Decompress the payload to obtain the frame scan data.

---

# Conceptual Representation

A TIMS frame can be represented as:

```

FRAME
├── Header (8 bytes)
│   ├── bin_size   (uint32)
│   └── num_scans  (uint32)
└── compressed_frame_data (uint8[])

```

Multiple such frames are concatenated to form the full dataset.
```
