# pgzstd_rs
 simple Rust implementation of manual zstd compression in postgres.

**do not attempt to rely on this yet. it is not ready**


## Testing 

This extension isn't packaged yet. to test, you'll want to run:
```
cargo pgrx run
```

once dropped into the shell, run
```
CREATE EXTENSION pgzstd_rs;
```

## Calling
### Compressing

`to_zstd` takes two arguments, data and compression level
```
SELECT to_zstd(
  '\x00000000000000808001000000000000000200000040000000000000002000000000000000000002000800000000080800010000000200000000000000080800100000000000000000800008000000008000080000000000000040000000000000000000020000000000020000000800000000000000001000000010200000000000000000000000002000001000000000000000000000008000000000000000000000000000080000000000000000000000200000000000000000000000000000008000001002000000000000000088000000000000000000000801000000000020000000410000000000000000000000000000000400000000008000008010040402', 
  19
);
```

### Decompressing
`from_zstd` takes one argument, compressed data
```
SELECT from_zstd('\x28b52ffd0068650200420a0d0ed0a5312b00e0f0089d4c266793141302d5f54372f1c196ffa16bebffb428e8f4bfcf9fb7fee9f69f83dbeffc17fff1f983aa7f0800407e402b202fa60e11f0c5338e60038c8c6206');
```

`from_maybe_zstd` is the same as `from_zstd`, except that if the data can't be decompressed, it is returned unaltered.

this allows for compressing large binary fields in-place and reading them safely.
```
SELECT from_maybe_zstd('\x28b52ffd0068650200420a0d0ed0a5312b00e0f0089d4c266793141302d5f54372f1c196ffa16bebffb428e8f4bfcf9fb7fee9f69f83dbeffc17fff1f983aa7f0800407e402b202fa60e11f0c5338e60038c8c6206');
```
