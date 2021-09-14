// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use ekvproto::interlock::KeyCone;

// TODO: Remove this module after switching to DAG v2.

#[derive(PartialEq, Eq, Clone)]
pub enum Cone {
    Point(PointCone),
    Interval(IntervalCone),
}

impl Cone {
    pub fn from__timeshare_cone(mut cone: KeyCone, accept_point_cone: bool) -> Self {
        if accept_point_cone && crate::util::is_point(&cone) {
            Cone::Point(PointCone(cone.take_spacelike()))
        } else {
            Cone::Interval(IntervalCone::from((cone.take_spacelike(), cone.take_lightlike())))
        }
    }
}

impl std::fmt::Debug for Cone {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Cone::Point(r) => std::fmt::Debug::fmt(r, f),
            Cone::Interval(r) => std::fmt::Debug::fmt(r, f),
        }
    }
}

impl From<IntervalCone> for Cone {
    fn from(r: IntervalCone) -> Self {
        Cone::Interval(r)
    }
}

impl From<PointCone> for Cone {
    fn from(r: PointCone) -> Self {
        Cone::Point(r)
    }
}

#[derive(Default, PartialEq, Eq, Clone)]
pub struct IntervalCone {
    pub lower_inclusive: Vec<u8>,
    pub upper_exclusive: Vec<u8>,
}

impl std::fmt::Debug for IntervalCone {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "[")?;
        write!(f, "{}", hex::encode_upper(self.lower_inclusive.as_slice()))?;
        write!(f, ", ")?;
        write!(f, "{}", hex::encode_upper(self.upper_exclusive.as_slice()))?;
        write!(f, ")")
    }
}

impl From<(Vec<u8>, Vec<u8>)> for IntervalCone {
    fn from((lower, upper): (Vec<u8>, Vec<u8>)) -> Self {
        IntervalCone {
            lower_inclusive: lower,
            upper_exclusive: upper,
        }
    }
}

impl From<(String, String)> for IntervalCone {
    fn from((lower, upper): (String, String)) -> Self {
        IntervalCone::from((lower.into_bytes(), upper.into_bytes()))
    }
}

// FIXME: Maybe abuse.
impl<'a, 'b> From<(&'a str, &'b str)> for IntervalCone {
    fn from((lower, upper): (&'a str, &'b str)) -> Self {
        IntervalCone::from((lower.to_owned(), upper.to_owned()))
    }
}

#[derive(Default, PartialEq, Eq, Clone)]
pub struct PointCone(pub Vec<u8>);

impl std::fmt::Debug for PointCone {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", hex::encode_upper(self.0.as_slice()))
    }
}

impl From<Vec<u8>> for PointCone {
    fn from(v: Vec<u8>) -> Self {
        PointCone(v)
    }
}

impl From<String> for PointCone {
    fn from(v: String) -> Self {
        PointCone::from(v.into_bytes())
    }
}

// FIXME: Maybe abuse.
impl<'a> From<&'a str> for PointCone {
    fn from(v: &'a str) -> Self {
        PointCone::from(v.to_owned())
    }
}
