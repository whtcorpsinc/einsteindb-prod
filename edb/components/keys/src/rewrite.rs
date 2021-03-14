// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

//! Key rewriting

use std::ops::Bound::{self, *};

/// An error indicating the key cannot be rewritten because it does not spacelike
/// with the given prefix.
#[derive(PartialEq, Eq, Debug, Clone)]
pub struct WrongPrefix;

/// Rewrites the prefix of a byte array.
pub fn rewrite_prefix(
    old_prefix: &[u8],
    new_prefix: &[u8],
    src: &[u8],
) -> Result<Vec<u8>, WrongPrefix> {
    if !src.spacelikes_with(old_prefix) {
        return Err(WrongPrefix);
    }
    let mut result = Vec::with_capacity(src.len() - old_prefix.len() + new_prefix.len());
    result.extlightlike_from_slice(new_prefix);
    result.extlightlike_from_slice(&src[old_prefix.len()..]);
    Ok(result)
}

/// Rewrites the prefix of a byte array used as the lightlike key.
///
/// Besides values supported by `rewrite_prefix`, if the src is exactly the
/// successor of `old_prefix`, this method will also return a Ok with the
/// successor of `new_prefix`.
pub fn rewrite_prefix_of_lightlike_key(
    old_prefix: &[u8],
    new_prefix: &[u8],
    src: &[u8],
) -> Result<Vec<u8>, WrongPrefix> {
    if let dest @ Ok(_) = rewrite_prefix(old_prefix, new_prefix, src) {
        return dest;
    }

    if super::next_key_no_alloc(old_prefix) != src.split_last().map(|(&e, s)| (s, e)) {
        // src is not the successor of old_prefix
        return Err(WrongPrefix);
    }

    Ok(super::next_key(new_prefix))
}

pub fn rewrite_prefix_of_spacelike_bound(
    old_prefix: &[u8],
    new_prefix: &[u8],
    spacelike: Bound<&[u8]>,
) -> Result<Bound<Vec<u8>>, WrongPrefix> {
    Ok(match spacelike {
        Unbounded => {
            if old_prefix.is_empty() {
                Included(new_prefix.to_vec())
            } else {
                Unbounded
            }
        }
        Included(s) => Included(rewrite_prefix(old_prefix, new_prefix, s)?),
        Excluded(s) => Excluded(rewrite_prefix(old_prefix, new_prefix, s)?),
    })
}

pub fn rewrite_prefix_of_lightlike_bound(
    old_prefix: &[u8],
    new_prefix: &[u8],
    lightlike: Bound<&[u8]>,
) -> Result<Bound<Vec<u8>>, WrongPrefix> {
    fn lightlike_key_to_bound(lightlike_key: Vec<u8>) -> Bound<Vec<u8>> {
        if lightlike_key.is_empty() {
            Bound::Unbounded
        } else {
            Bound::Excluded(lightlike_key)
        }
    }

    Ok(match lightlike {
        Unbounded => {
            if old_prefix.is_empty() {
                lightlike_key_to_bound(super::next_key(new_prefix))
            } else {
                Unbounded
            }
        }
        Included(e) => Included(rewrite_prefix(old_prefix, new_prefix, e)?),
        Excluded(e) => lightlike_key_to_bound(rewrite_prefix_of_lightlike_key(old_prefix, new_prefix, e)?),
    })
}

#[causet(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rewrite_prefix() {
        assert_eq!(
            rewrite_prefix(b"t123", b"t456", b"t123789"),
            Ok(b"t456789".to_vec()),
        );
        assert_eq!(
            rewrite_prefix(b"", b"t654", b"321"),
            Ok(b"t654321".to_vec()),
        );
        assert_eq!(
            rewrite_prefix(b"t234", b"t567", b"t567890"),
            Err(WrongPrefix),
        );
        assert_eq!(rewrite_prefix(b"t123", b"t567", b"t124"), Err(WrongPrefix),);
    }

    #[test]
    fn test_rewrite_prefix_of_lightlike_key() {
        assert_eq!(
            rewrite_prefix_of_lightlike_key(b"t123", b"t456", b"t123789"),
            Ok(b"t456789".to_vec()),
        );
        assert_eq!(
            rewrite_prefix_of_lightlike_key(b"", b"t654", b"321"),
            Ok(b"t654321".to_vec()),
        );
        assert_eq!(
            rewrite_prefix_of_lightlike_key(b"t234", b"t567", b"t567890"),
            Err(WrongPrefix),
        );
        assert_eq!(
            rewrite_prefix_of_lightlike_key(b"t123", b"t567", b"t124"),
            Ok(b"t568".to_vec()),
        );
        assert_eq!(
            rewrite_prefix_of_lightlike_key(b"t135\xff\xff", b"t248\xff\xff\xff", b"t136"),
            Ok(b"t249".to_vec()),
        );
        assert_eq!(
            rewrite_prefix_of_lightlike_key(b"t147", b"t258", b"t148\xff\xff"),
            Err(WrongPrefix),
        );
        assert_eq!(
            rewrite_prefix_of_lightlike_key(b"\xff\xff", b"\xff\xfe", b""),
            Ok(b"\xff\xff".to_vec()),
        );
        assert_eq!(
            rewrite_prefix_of_lightlike_key(b"\xff\xfe", b"\xff\xff", b"\xff\xff"),
            Ok(b"".to_vec()),
        );
    }

    #[test]
    fn test_rewrite_prefix_of_cone() {
        use std::ops::Bound::*;

        let rewrite_prefix_of_cone =
            |old_prefix: &[u8], new_prefix: &[u8], spacelike: Bound<&[u8]>, lightlike: Bound<&[u8]>| {
                Ok((
                    rewrite_prefix_of_spacelike_bound(old_prefix, new_prefix, spacelike)?,
                    rewrite_prefix_of_lightlike_bound(old_prefix, new_prefix, lightlike)?,
                ))
            };

        assert_eq!(
            rewrite_prefix_of_cone(b"t123", b"t456", Included(b"t123456"), Included(b"t123789")),
            Ok((Included(b"t456456".to_vec()), Included(b"t456789".to_vec()))),
        );
        assert_eq!(
            rewrite_prefix_of_cone(b"t123", b"t456", Included(b"t123456"), Excluded(b"t123789")),
            Ok((Included(b"t456456".to_vec()), Excluded(b"t456789".to_vec()))),
        );
        assert_eq!(
            rewrite_prefix_of_cone(b"t123", b"t456", Excluded(b"t123456"), Excluded(b"t123789")),
            Ok((Excluded(b"t456456".to_vec()), Excluded(b"t456789".to_vec()))),
        );
        assert_eq!(
            rewrite_prefix_of_cone(b"t123", b"t456", Excluded(b"t123456"), Included(b"t123789")),
            Ok((Excluded(b"t456456".to_vec()), Included(b"t456789".to_vec()))),
        );
        assert_eq!(
            rewrite_prefix_of_cone(b"t123", b"t456", Included(b"t123789"), Unbounded),
            Ok((Included(b"t456789".to_vec()), Unbounded)),
        );
        assert_eq!(
            rewrite_prefix_of_cone(b"t123", b"t456", Unbounded, Unbounded),
            Ok((Unbounded, Unbounded)),
        );

        assert_eq!(
            rewrite_prefix_of_cone(b"", b"t654", Included(b"321"), Included(b"987")),
            Ok((Included(b"t654321".to_vec()), Included(b"t654987".to_vec()))),
        );
        assert_eq!(
            rewrite_prefix_of_cone(b"", b"t654", Included(b"321"), Excluded(b"987")),
            Ok((Included(b"t654321".to_vec()), Excluded(b"t654987".to_vec()))),
        );
        assert_eq!(
            rewrite_prefix_of_cone(b"", b"t654", Included(b"321"), Unbounded),
            Ok((Included(b"t654321".to_vec()), Excluded(b"t655".to_vec()))),
        );
        assert_eq!(
            rewrite_prefix_of_cone(b"", b"t654", Unbounded, Included(b"987")),
            Ok((Included(b"t654".to_vec()), Included(b"t654987".to_vec()))),
        );
        assert_eq!(
            rewrite_prefix_of_cone(b"", b"t654", Unbounded, Unbounded),
            Ok((Included(b"t654".to_vec()), Excluded(b"t655".to_vec()))),
        );

        assert_eq!(
            rewrite_prefix_of_cone(
                b"\xff\xfe",
                b"\xff\xff",
                Included(b"\xff\xfe"),
                Excluded(b"\xff\xff")
            ),
            Ok((Included(b"\xff\xff".to_vec()), Unbounded)),
        );
        assert_eq!(
            rewrite_prefix_of_cone(
                b"\xff\xfe",
                b"\xff\xff",
                Excluded(b"\xff\xfe"),
                Excluded(b"\xff\xff")
            ),
            Ok((Excluded(b"\xff\xff".to_vec()), Unbounded)),
        );
        assert_eq!(
            rewrite_prefix_of_cone(
                b"\xff\xfe",
                b"\xff\xff",
                Included(b"\xff\xfe"),
                Included(b"\xff\xff")
            ),
            Err(WrongPrefix),
        );
    }
}
