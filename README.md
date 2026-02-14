# 🐾 pounce

**Arrow-native ADBC driver for SQL Server. Zero-copy. Zero shame.**

Pounce is the [ADBC](https://arrow.apache.org/adbc/) driver for SQL Server in the CopyCat ecosystem. It speaks TDS via [tabby](https://github.com/copycatdb/tabby) and returns data as Apache Arrow record batches through zero-copy FFI.

## Architecture

```
┌────────────────────┐
│   Python (PyArrow)  │
├────────────────────┤
│   pounce (PyO3)     │  ← Arrow FFI, type conversion
├────────────────────┤
│   tabby             │  ← TDS 7.4+ protocol (git dep)
├────────────────────┤
│   TCP / TLS         │
└────────────────────┘
        ↕
   SQL Server
```

## Features

- **Arrow-native** — results come back as Arrow record batches, not rows-of-tuples
- **Zero-copy FFI** — Arrow arrays cross the Rust→Python boundary without copying
- **Full type support** — all SQL Server types mapped to Arrow equivalents
- **Transaction management** — autocommit, begin/commit/rollback
- **Bulk ingest** — load PyArrow tables directly into SQL Server
- **TLS encryption** — rustls by default

## Part of CopyCat 🐱

| Crate | Role |
|-------|------|
| [tabby](https://github.com/copycatdb/tabby) | TDS 7.4+ protocol engine |
| **pounce** | Arrow-native ADBC driver |
| [hiss](https://github.com/copycatdb/hiss) | Python DB-API driver |
| [whiskers](https://github.com/copycatdb/whiskers) | ODBC driver |
| [kibble](https://github.com/copycatdb/kibble) | Node.js driver |
| [claw](https://github.com/copycatdb/claw) | Rust client |
| [nuzzle](https://github.com/copycatdb/nuzzle) | .NET driver |

## License

MIT
