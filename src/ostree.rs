use byteorder::{ByteOrder, LittleEndian, NativeEndian};
use failure::Fail;
use futures::future;
use futures::future::Either;
use futures::Future;
use serde::{Deserialize, Serialize};
use std::fs;
use std::io::Read;
use std::num::NonZeroUsize;
use std::os::unix::process::CommandExt as UnixCommandExt;
use std::path;
use std::path::PathBuf;
use std::process::Command;
use std::str;
use std::{collections::HashMap, path::Path};
use tokio_process::CommandExt;
use walkdir::WalkDir;

#[derive(Fail, Debug, Clone, Eq, PartialEq)]
pub enum OstreeError {
    #[fail(display = "No such ref: {}", _0)]
    NoSuchRef(String),
    #[fail(display = "No such commit: {}", _0)]
    NoSuchCommit(String),
    #[fail(display = "No such object: {}", _0)]
    NoSuchObject(String),
    #[fail(display = "Invalid utf8 string")]
    InvalidUtf8,
    #[fail(display = "Command {} failed to start: {}", _0, _1)]
    ExecFailed(String, String),
    #[fail(display = "Command {} exited unsucessfully with stderr: {}", _0, _1)]
    CommandFailed(String, String),
    #[fail(display = "Internal Error: {}", _0)]
    InternalError(String),
}

pub type OstreeResult<T> = Result<T, OstreeError>;

#[derive(Debug)]
pub struct OstreeCommit {
    pub metadata: HashMap<String, Variant>,
    pub parent: Option<String>,
    pub subject: String,
    pub body: String,
    pub timestamp: u64,
    pub root_tree: String,
    pub root_metadata: String,
}

#[derive(Debug)]
pub struct OstreeDeltaSuperblock {
    pub metadata: HashMap<String, Variant>,
    pub commit: OstreeCommit,
}

fn is_base_type(byte: u8) -> bool {
    let c = byte as char;
    c == 'b'
        || c == 'y'
        || c == 'n'
        || c == 'q'
        || c == 'i'
        || c == 'u'
        || c == 'x'
        || c == 't'
        || c == 's'
        || c == 'o'
        || c == 'g'
}

fn type_string_element_len(type_string: &str) -> Option<usize> {
    if type_string.is_empty() {
        return None;
    }
    let bytes = type_string.as_bytes();
    let c = bytes[0];
    if is_base_type(c) || c == b'v' {
        return Some(1);
    }
    match c as char {
        'm' | 'a' => type_string_element_len(&type_string[1..]).map(|len| 1 + len),
        '{' => {
            if type_string.len() < 3 || !is_base_type(bytes[1]) {
                return None;
            }
            if let Some(len) = type_string_element_len(&type_string[2..]) {
                if type_string.len() > 2 + len && bytes[2 + len] != b'{' {
                    Some(3 + len)
                } else {
                    None
                }
            } else {
                None
            }
        }
        '(' => {
            let mut pos: usize = 1;
            loop {
                if type_string.len() <= pos {
                    return None;
                }
                if bytes[pos] == b')' {
                    return Some(pos + 1);
                }
                if let Some(len) = type_string_element_len(&type_string[pos..]) {
                    pos += len;
                } else {
                    return None;
                }
            }
        }
        _ => None,
    }
}

fn type_string_split(type_string: &str) -> Option<(&str, &str)> {
    type_string_element_len(type_string).map(|len| (&type_string[0..len], &type_string[len..]))
}

#[derive(Debug)]
enum VariantSize {
    Fixed(NonZeroUsize),
    Variable,
}

#[derive(Debug)]
struct VariantFieldInfo {
    size: VariantSize,
    alignment: usize,
}

#[derive(Debug)]
struct SubVariant<'a> {
    type_string: &'a str,
    data: &'a [u8],
}

#[derive(Debug)]
pub struct Variant {
    pub type_string: String,
    data: Vec<u8>,
}

impl Variant {
    fn new(type_string: String, data: Vec<u8>) -> OstreeResult<Variant> {
        match type_string_element_len(&type_string) {
            None => {
                return Err(OstreeError::InternalError(format!(
                    "Invalid type string '{}'",
                    type_string
                )));
            }
            Some(len) => {
                if len != type_string.len() {
                    return Err(OstreeError::InternalError(format!(
                        "Leftover text in type string '{}'",
                        type_string
                    )));
                }
            }
        };

        Ok(Variant { type_string, data })
    }

    fn root(&self) -> SubVariant<'_> {
        SubVariant {
            type_string: &self.type_string,
            data: &self.data,
        }
    }

    pub fn as_string(&self) -> OstreeResult<String> {
        return self.root().parse_as_string();
    }

    pub fn as_string_vec(&self) -> OstreeResult<Vec<String>> {
        return self.root().parse_as_string_vec();
    }

    pub fn as_u64(&self) -> OstreeResult<u64> {
        return self.root().parse_as_u64();
    }

    pub fn as_i32(&self) -> OstreeResult<i32> {
        return self.root().parse_as_i32();
    }

    pub fn as_i32_le(&self) -> OstreeResult<i32> {
        return self.root().parse_as_i32_le();
    }

    pub fn as_bytes(&self) -> &[u8] {
        return self.root().parse_as_bytes();
    }
}

impl<'a> SubVariant<'a> {
    fn copy(&self) -> Variant {
        Variant {
            type_string: self.type_string.to_string(),
            data: self.data.to_vec(),
        }
    }

    fn framing_size(&self) -> usize {
        let len = self.data.len() as u64;
        if len == 0 {
            0
        } else if len <= ::std::u8::MAX as u64 {
            1
        } else if len <= ::std::u16::MAX as u64 {
            2
        } else if len <= ::std::u32::MAX as u64 {
            4
        } else {
            8
        }
    }
    fn read_frame_offset(&self, offset: usize, framing_size: usize) -> OstreeResult<usize> {
        if offset + framing_size > self.data.len() {
            return Err(OstreeError::InternalError(format!(
                "Framing error: can't read frame offset at {}",
                offset
            )));
        }
        let data = &self.data[offset..offset + framing_size];
        let offset = match framing_size {
            0 => 0,
            1 => usize::from(data[0]),
            2 => usize::from(LittleEndian::read_u16(data)),
            4 => LittleEndian::read_u32(data) as usize,
            8 => {
                let len64 = LittleEndian::read_u64(data);
                if len64 > ::std::usize::MAX as u64 {
                    return Err(OstreeError::InternalError(
                        "Framing error: To large framing size fror usize".to_string(),
                    ));
                }
                len64 as usize
            }
            _ => {
                return Err(OstreeError::InternalError(format!(
                    "Framing error: Unexpected framing size {}",
                    framing_size
                )))
            }
        };
        if offset > self.data.len() {
            return Err(OstreeError::InternalError(format!(
                "Framing error: out of bounds offset at {}",
                offset
            )));
        };
        Ok(offset)
    }

    fn pad(&self, cur: usize, alignment: usize) -> usize {
        if alignment == 0 {
            cur
        } else {
            let offset = cur;
            cur + (alignment - (offset % alignment)) % alignment
        }
    }

    fn subset(
        &self,
        start: usize,
        end: usize,
        type_string: &'a str,
    ) -> OstreeResult<SubVariant<'a>> {
        if end < start || end > self.data.len() {
            return Err(OstreeError::InternalError(format!(
                "Framing error: subset {}-{} out of bounds for {:?}",
                start, end, self
            )));
        }
        Ok(SubVariant {
            type_string,
            data: &self.data[start..end],
        })
    }

    fn checked_sub(&self, a: usize, b: usize) -> OstreeResult<usize> {
        if b > a {
            Err(OstreeError::InternalError(
                "Framing error: negative checked_sub".to_string(),
            ))
        } else {
            Ok(a - b)
        }
    }

    fn parse_as_tuple(&self, fields: &[VariantFieldInfo]) -> OstreeResult<Vec<SubVariant<'a>>> {
        let mut result = Vec::new();

        let t = self.type_string.as_bytes()[0] as char;
        if t != '(' && t != '{' {
            return Err(OstreeError::InternalError(format!(
                "Not a dictionary: {}",
                self.type_string
            )));
        }

        let mut type_string_rest = &self.type_string[1..];

        let framing_size = self.framing_size();
        let mut frame_offset = self.data.len();

        let mut next: usize = 0;
        for i in 0..fields.len() {
            let field = &fields[i];

            let field_type = if let Some((t, r)) = type_string_split(type_string_rest) {
                type_string_rest = r;
                t
            } else {
                return Err(OstreeError::InternalError(format!(
                    "Invalid type: {}",
                    type_string_rest
                )));
            };

            next = self.pad(next, field.alignment);

            let field_size = match field.size {
                VariantSize::Fixed(size) => usize::from(size),
                VariantSize::Variable => {
                    let end = if i == fields.len() - 1 {
                        frame_offset
                    } else {
                        frame_offset = self.checked_sub(frame_offset, framing_size)?;
                        self.read_frame_offset(frame_offset, framing_size)?
                    };
                    self.checked_sub(end, next)?
                }
            };

            let sub = self.subset(next, next + field_size, field_type)?;
            result.push(sub);
            next += field_size;
        }

        Ok(result)
    }

    fn parse_as_variable_width_array(
        &self,
        element_alignment: usize,
    ) -> OstreeResult<Vec<SubVariant<'a>>> {
        let t = self.type_string.as_bytes()[0] as char;
        if t != 'a' {
            return Err(OstreeError::InternalError(format!(
                "Not an array: {}",
                self.type_string
            )));
        }

        let size = self.data.len();
        if size == 0 {
            return Ok(Vec::new()); // Empty array
        }

        let element_type = &self.type_string[1..];
        let framing_size = self.framing_size();
        let last_offset = self.read_frame_offset(self.data.len() - framing_size, framing_size)?;
        let length = (size - last_offset) / framing_size;
        let frame_offsets = self.data.len() - length * framing_size;

        let mut result = Vec::with_capacity(length);

        let mut last_end: usize = 0;
        for i in 0..length {
            let start = self.pad(last_end, element_alignment);
            let end = self.read_frame_offset(frame_offsets + i * framing_size, framing_size)?;

            let sub = self.subset(start, end, element_type)?;
            result.push(sub);
            last_end = end;
        }

        Ok(result)
    }

    fn parse_as_variant(&self) -> OstreeResult<SubVariant<'a>> {
        if self.type_string != "v" {
            return Err(OstreeError::InternalError(format!(
                "Variant type '{}' not a variant",
                self.type_string
            )));
        }

        if self.data.is_empty() {
            return self.subset(0, 0, "()");
        }

        let parts: Vec<&'a [u8]> = self.data.rsplitn(2, |&x| x == 0).collect();
        if parts.len() != 2 {
            return Err(OstreeError::InternalError(
                "No type string in variant".to_string(),
            ));
        }

        if let Ok(type_string) = str::from_utf8(parts[0]) {
            self.subset(0, parts[1].len(), type_string)
        } else {
            Err(OstreeError::InvalidUtf8)
        }
    }

    fn parse_as_asv_element(&self) -> OstreeResult<(String, SubVariant<'a>)> {
        let fields = vec![
            // 0 - s - key
            VariantFieldInfo {
                size: VariantSize::Variable,
                alignment: 0,
            },
            // 1 - v - value
            VariantFieldInfo {
                size: VariantSize::Variable,
                alignment: 8,
            },
        ];
        let kv = self.parse_as_tuple(&fields)?;
        let key = kv[0].parse_as_string()?;
        let val = kv.into_iter().nth(1).unwrap();
        Ok((key, val))
    }

    fn parse_as_asv(&self) -> OstreeResult<HashMap<String, Variant>> {
        let mut res = HashMap::new();

        for elt in self.parse_as_variable_width_array(8)? {
            let (k, v) = elt.parse_as_asv_element()?;
            let vv = v.parse_as_variant()?;
            res.insert(k, vv.copy());
        }
        Ok(res)
    }

    fn parse_as_string(&self) -> OstreeResult<String> {
        if self.type_string != "s" {
            return Err(OstreeError::InternalError(format!(
                "Variant type '{}' not a string",
                self.type_string
            )));
        }
        let without_nul = &self.data[0..self.data.len() - 1];
        if let Ok(str) = str::from_utf8(without_nul) {
            Ok(str.to_string())
        } else {
            Err(OstreeError::InvalidUtf8)
        }
    }

    fn parse_as_string_vec(&self) -> OstreeResult<Vec<String>> {
        if self.type_string != "as" {
            return Err(OstreeError::InternalError(format!(
                "Variant type '{}' not an array of strings",
                self.type_string
            )));
        }
        let array = self.parse_as_variable_width_array(0)?;
        return array.iter().map(|v| v.parse_as_string()).collect();
    }

    fn parse_as_bytes(&self) -> &'a [u8] {
        self.data
    }

    fn parse_as_u64(&self) -> OstreeResult<u64> {
        if self.type_string != "t" {
            return Err(OstreeError::InternalError(format!(
                "Variant type '{}' not a u64",
                self.type_string
            )));
        }
        if self.data.len() != 8 {
            return Err(OstreeError::InternalError(format!(
                "Wrong length {} for u64",
                self.data.len()
            )));
        }
        Ok(NativeEndian::read_u64(self.data))
    }

    fn parse_as_i32(&self) -> OstreeResult<i32> {
        if self.type_string != "i" {
            return Err(OstreeError::InternalError(format!(
                "Variant type '{}' not a i32",
                self.type_string
            )));
        }
        Ok(NativeEndian::read_i32(self.data))
    }

    fn parse_as_i32_le(&self) -> OstreeResult<i32> {
        if self.type_string != "i" {
            return Err(OstreeError::InternalError(format!(
                "Variant type '{}' not a i32",
                self.type_string
            )));
        }
        Ok(LittleEndian::read_i32(self.data))
    }
}

fn bytes_to_object(bytes: &[u8]) -> String {
    hex::encode(bytes)
}

fn object_to_bytes(object: &str) -> OstreeResult<Vec<u8>> {
    hex::decode(object)
        .map_err(|e| OstreeError::InternalError(format!("Invalid object '{}: {}'", object, e)))
}

fn maybe_bytes_to_object(bytes: &[u8]) -> Option<String> {
    if bytes.is_empty() {
        None
    } else {
        Some(bytes_to_object(bytes))
    }
}

fn get_ref_path(repo_path: &path::Path) -> path::PathBuf {
    let mut ref_dir = std::env::current_dir().unwrap_or_default();
    ref_dir.push(repo_path);
    ref_dir.push("refs/heads");
    ref_dir
}

fn get_deltas_path(repo_path: &path::Path) -> path::PathBuf {
    let mut ref_dir = std::env::current_dir().unwrap_or_default();
    ref_dir.push(repo_path);
    ref_dir.push("deltas");
    ref_dir
}

fn get_tmp_deltas_path(repo_path: &path::Path) -> path::PathBuf {
    let mut ref_dir = std::env::current_dir().unwrap_or_default();
    ref_dir.push(repo_path);
    ref_dir.push("tmp/deltas");
    ref_dir
}

fn get_object_path(repo_path: &path::Path, object: &str, object_type: &str) -> path::PathBuf {
    let mut path = std::env::current_dir().unwrap_or_default();
    path.push(repo_path);
    path.push("objects");
    path.push(&object[0..2]);
    path.push(format!("{}.{}", &object[2..], object_type));
    path
}

fn parse_commit(variant: &SubVariant) -> OstreeResult<OstreeCommit> {
    let ostree_commit_fields = vec![
        // 0 - a{sv} - Metadata
        VariantFieldInfo {
            size: VariantSize::Variable,
            alignment: 8,
        },
        // 1 - ay - parent checksum (empty string for initial)
        VariantFieldInfo {
            size: VariantSize::Variable,
            alignment: 0,
        },
        // 2- a(say) - Related objects
        VariantFieldInfo {
            size: VariantSize::Variable,
            alignment: 0,
        },
        // 3 - s - subject
        VariantFieldInfo {
            size: VariantSize::Variable,
            alignment: 0,
        },
        // 4- s - body
        VariantFieldInfo {
            size: VariantSize::Variable,
            alignment: 0,
        },
        // 5- t - Timestamp in seconds since the epoch (UTC, big-endian)
        VariantFieldInfo {
            size: VariantSize::Fixed(std::num::NonZeroUsize::new(8).unwrap()),
            alignment: 8,
        },
        // 6- ay - Root tree contents
        VariantFieldInfo {
            size: VariantSize::Variable,
            alignment: 0,
        },
        // 7- ay - Root tree metadata
        VariantFieldInfo {
            size: VariantSize::Variable,
            alignment: 0,
        },
    ];

    let commit = variant.parse_as_tuple(&ostree_commit_fields)?;
    let metadata = commit[0].parse_as_asv()?;

    Ok(OstreeCommit {
        metadata,
        parent: maybe_bytes_to_object(commit[1].parse_as_bytes()),
        subject: commit[3].parse_as_string()?,
        body: commit[4].parse_as_string()?,
        timestamp: u64::from_be(commit[5].parse_as_u64()?),
        root_tree: bytes_to_object(commit[6].parse_as_bytes()),
        root_metadata: bytes_to_object(commit[7].parse_as_bytes()),
    })
}

/* This is like basename, but also includes the parent dir because in
 * ostree object and delta part filenames that is the first to letters
 * of the ID, which we don't want to miss.
 * Also, this is mainly used for debug/errors, so converts to string.
 */
fn get_dir_and_basename(path: &path::Path) -> String {
    let mut res = String::new();
    if let Some(parent_name) = path
        .parent()
        .and_then(|parent| parent.file_name())
        .and_then(|file_name| file_name.to_str())
    {
        res.push_str(parent_name);
        res.push('/');
    }
    res.push_str(path.file_name().and_then(|s| s.to_str()).unwrap_or("?"));
    res
}

pub fn load_commit_file(path: &path::Path) -> OstreeResult<OstreeCommit> {
    let mut fp =
        fs::File::open(path).map_err(|_e| OstreeError::NoSuchCommit(get_dir_and_basename(path)))?;

    let mut contents = vec![];
    fp.read_to_end(&mut contents).map_err(|_e| {
        OstreeError::InternalError(format!("Invalid commit {}", get_dir_and_basename(path)))
    })?;

    let variant = Variant::new("(a{sv}aya(say)sstayay)".to_string(), contents)?;

    return parse_commit(&variant.root());
}

pub fn get_commit(repo_path: &path::Path, commit: &str) -> OstreeResult<OstreeCommit> {
    let path = get_object_path(repo_path, commit, "commit");
    load_commit_file(&path)
}

pub fn load_delta_superblock_file(path: &path::Path) -> OstreeResult<OstreeDeltaSuperblock> {
    let mut fp =
        fs::File::open(path).map_err(|_e| OstreeError::NoSuchObject(get_dir_and_basename(path)))?;

    let mut contents = vec![];
    fp.read_to_end(&mut contents).map_err(|_e| {
        OstreeError::InternalError(format!(
            "Invalid delta superblock {}",
            get_dir_and_basename(path)
        ))
    })?;

    let ostree_superblock_fields = vec![
        // 0 - "a{sv}", - Metadata
        VariantFieldInfo {
            size: VariantSize::Variable,
            alignment: 8,
        },
        // 1 - "t", - timestamp
        VariantFieldInfo {
            size: VariantSize::Fixed(std::num::NonZeroUsize::new(8).unwrap()),
            alignment: 8,
        },
        // 2 - "ay" - from checksum
        VariantFieldInfo {
            size: VariantSize::Variable,
            alignment: 0,
        },
        // 3 - "ay" - to checksum
        VariantFieldInfo {
            size: VariantSize::Variable,
            alignment: 0,
        },
        // 4 - "(a{sv}aya(say)sstayay)" - commit object
        VariantFieldInfo {
            size: VariantSize::Variable,
            alignment: 8,
        },
        // 5 - "ay" -Prerequisite deltas
        VariantFieldInfo {
            size: VariantSize::Variable,
            alignment: 0,
        },
        // 6 - "a(uayttay)" -Delta objects
        VariantFieldInfo {
            size: VariantSize::Variable,
            alignment: 8,
        },
        // 7 - "a(yaytt)" - Fallback objects
        VariantFieldInfo {
            size: VariantSize::Variable,
            alignment: 8,
        },
    ];

    let variant = Variant::new(
        "(a{sv}tayay(a{sv}aya(say)sstayay)aya(uayttay)a(yaytt))".to_string(),
        contents,
    )?;
    let container = variant.root();
    let superblock = container.parse_as_tuple(&ostree_superblock_fields)?;

    let metadata = superblock[0].parse_as_asv()?;
    let commit = parse_commit(&superblock[4])?;

    Ok(OstreeDeltaSuperblock { metadata, commit })
}

pub fn get_delta_superblock(
    repo_path: &path::Path,
    delta: &str,
) -> OstreeResult<OstreeDeltaSuperblock> {
    let mut path = get_deltas_path(repo_path);
    path.push(&delta[0..2]);
    path.push(&delta[2..]);
    path.push("superblock");

    load_delta_superblock_file(&path)
}

pub fn parse_ref(repo_path: &path::Path, ref_name: &str) -> OstreeResult<String> {
    let mut ref_dir = get_ref_path(repo_path);
    ref_dir.push(ref_name);
    let commit = fs::read_to_string(ref_dir)
        .map_err(|_e| OstreeError::NoSuchRef(ref_name.to_string()))?
        .trim_end()
        .to_string();
    Ok(commit)
}

pub fn list_refs(repo_path: &path::Path, prefix: &str) -> Vec<String> {
    let mut ref_dir = get_ref_path(repo_path);
    let path_prefix = &ref_dir.clone();
    ref_dir.push(prefix);

    WalkDir::new(&ref_dir)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| !e.file_type().is_dir())
        .filter_map(|e| {
            e.path()
                .strip_prefix(path_prefix)
                .map(|p| p.to_path_buf())
                .ok()
        })
        .filter_map(|p| p.to_str().map(|s| s.to_string()))
        .collect()
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Hash, Clone)]
pub struct Delta {
    pub from: Option<String>,
    pub to: String,
}

fn delta_part_to_hex(part: &str) -> OstreeResult<String> {
    let bytes = base64::decode(&part.replace('_', "/")).map_err(|err| {
        OstreeError::InternalError(format!("Invalid delta part name '{}': {}", part, err))
    })?;
    Ok(bytes_to_object(&bytes))
}

fn hex_to_delta_part(hex: &str) -> OstreeResult<String> {
    let bytes = object_to_bytes(hex)?;
    let part = base64::encode_config(&bytes, base64::STANDARD_NO_PAD);
    Ok(part.replace('/', "_"))
}

impl Delta {
    pub fn new(from: Option<&str>, to: &str) -> Delta {
        Delta {
            from: from.map(|s| s.to_string()),
            to: to.to_string(),
        }
    }
    pub fn from_name(name: &str) -> OstreeResult<Delta> {
        let parts: Vec<&str> = name.split('-').collect();
        if parts.len() == 1 {
            Ok(Delta {
                from: None,
                to: delta_part_to_hex(parts[0])?,
            })
        } else {
            Ok(Delta {
                from: Some(delta_part_to_hex(parts[0])?),
                to: delta_part_to_hex(parts[1])?,
            })
        }
    }

    pub fn to_name(&self) -> OstreeResult<String> {
        let mut name = String::new();

        if let Some(ref from) = self.from {
            name.push_str(&hex_to_delta_part(from)?);
            name.push('-');
        }
        name.push_str(&hex_to_delta_part(&self.to)?);
        Ok(name)
    }

    pub fn delta_path(&self, repo_path: &path::Path) -> OstreeResult<path::PathBuf> {
        let mut path = get_deltas_path(repo_path);
        let name = self.to_name()?;
        path.push(&name[0..2]);
        path.push(&name[2..]);
        Ok(path)
    }

    pub fn tmp_delta_path(&self, repo_path: &path::Path) -> OstreeResult<path::PathBuf> {
        let mut path = get_tmp_deltas_path(repo_path);
        let name = self.to_name()?;
        path.push(&name[0..2]);
        path.push(&name[2..]);
        Ok(path)
    }
}

impl std::fmt::Display for Delta {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(
            f,
            "{}-{}",
            self.from.as_ref().unwrap_or(&"nothing".to_string()),
            self.to
        )
    }
}
#[cfg(test)]
mod tests {
    // Note this useful idiom: importing names from outer (for mod tests) scope.
    use super::*;

    #[test]
    fn test_variant_type_strings() {
        assert_eq!(type_string_element_len("1"), None);
        assert_eq!(type_string_element_len("i"), Some(1));
        assert_eq!(type_string_element_len("s"), Some(1));
        assert_eq!(type_string_element_len("asas"), Some(2));
        assert_eq!(type_string_split("asas"), Some(("as", "as")));
        assert_eq!(type_string_element_len("(ssas)as"), Some(6));
        assert_eq!(type_string_element_len("(ssas"), None);
        assert_eq!(type_string_element_len("(ssas)"), Some(6));
        assert_eq!(type_string_element_len("(sa{sv}sas)ias"), Some(11));
        assert_eq!(type_string_split("(ssas)ii"), Some(("(ssas)", "ii")));
        assert_eq!(type_string_split("a{sv}as"), Some(("a{sv}", "as")));
        assert_eq!(type_string_split("a{vv}as"), None);
    }

    #[test]
    fn test_delta_name() {
        assert_eq!(
            delta_part_to_hex("OkiocD9GLq_Nt660BvWyrH8G62dAvtLv7RPqngWqf5c"),
            Ok("3a48a8703f462eafcdb7aeb406f5b2ac7f06eb6740bed2efed13ea9e05aa7f97".to_string())
        );
        assert_eq!(
            hex_to_delta_part("3a48a8703f462eafcdb7aeb406f5b2ac7f06eb6740bed2efed13ea9e05aa7f97"),
            Ok("OkiocD9GLq_Nt660BvWyrH8G62dAvtLv7RPqngWqf5c".to_string())
        );
        assert_eq!(
            Delta::from_name("OkiocD9GLq_Nt660BvWyrH8G62dAvtLv7RPqngWqf5c"),
            Ok(Delta {
                from: None,
                to: "3a48a8703f462eafcdb7aeb406f5b2ac7f06eb6740bed2efed13ea9e05aa7f97".to_string()
            })
        );
        assert_eq!(Delta::from_name("OkiocD9GLq_Nt660BvWyrH8G62dAvtLv7RPqngWqf5c-3dpOrJG4MNyKHDDGXHpH_zd9NXugnexr5jpvSFQ77S4"),
                   Ok(Delta { from: Some("3a48a8703f462eafcdb7aeb406f5b2ac7f06eb6740bed2efed13ea9e05aa7f97".to_string()), to: "ddda4eac91b830dc8a1c30c65c7a47ff377d357ba09dec6be63a6f48543bed2e".to_string() }));
    }
}

pub fn list_deltas(repo_path: &path::Path) -> Vec<Delta> {
    let deltas_dir = get_deltas_path(repo_path);

    WalkDir::new(&deltas_dir)
        .min_depth(2)
        .max_depth(2)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_dir())
        .map(|e| {
            format!(
                "{}{}",
                e.path()
                    .parent()
                    .unwrap()
                    .file_name()
                    .unwrap()
                    .to_string_lossy(),
                e.file_name().to_string_lossy()
            )
        })
        .filter_map(|name| Delta::from_name(&name).ok())
        .collect()
}

pub fn calc_deltas_for_ref(repo_path: &path::Path, ref_name: &str, depth: u32) -> Vec<Delta> {
    let mut res = Vec::new();

    let to_commit_res = parse_ref(repo_path, ref_name);
    if to_commit_res.is_err() {
        return res;
    }
    let to_commit = to_commit_res.unwrap();

    let mut from_commit: Option<String> = None;
    for _i in 0..depth {
        if let Ok(commitinfo) = get_commit(repo_path, from_commit.as_ref().unwrap_or(&to_commit)) {
            res.push(Delta::new(from_commit.as_deref(), &to_commit));
            from_commit = commitinfo.parent;
            if from_commit == None {
                break;
            }
        } else {
            break;
        }
    }

    res
}

fn result_from_output(output: std::process::Output, command: &str) -> Result<(), OstreeError> {
    if !output.status.success() {
        Err(OstreeError::CommandFailed(
            command.to_string(),
            String::from_utf8_lossy(&output.stderr).trim().to_string(),
        ))
    } else {
        Ok(())
    }
}

pub fn pull_commit_async(
    n_retries: i32,
    repo_path: PathBuf,
    url: String,
    commit: String,
) -> Box<dyn Future<Item = (), Error = OstreeError>> {
    Box::new(future::loop_fn(n_retries, move |count| {
        let mut cmd = Command::new("ostree");
        unsafe {
            cmd.pre_exec(|| {
                // Setsid in the child to avoid SIGINT on server killing
                // child and breaking the graceful shutdown
                libc::setsid();
                Ok(())
            });
        }

        cmd.arg(&format!("--repo={}", &repo_path.to_str().unwrap()))
            .arg("pull")
            .arg(&format!("--url={}", url))
            .arg("upstream")
            .arg(&commit);

        log::info!("Pulling commit {}", commit);
        let commit_clone = commit.clone();
        cmd.output_async()
            .map_err(|e| OstreeError::ExecFailed("ostree pull".to_string(), e.to_string()))
            .and_then(|output| result_from_output(output, "ostree pull"))
            .then(move |r| match r {
                Ok(res) => Ok(future::Loop::Break(res)),
                Err(e) => {
                    if count > 1 {
                        log::warn!(
                            "Pull error, retrying commit {}: {}",
                            commit_clone,
                            e.to_string()
                        );
                        Ok(future::Loop::Continue(count - 1))
                    } else {
                        Err(e)
                    }
                }
            })
    }))
}

pub fn pull_delta_async(
    n_retries: i32,
    repo_path: &Path,
    url: &str,
    delta: &Delta,
) -> Box<dyn Future<Item = (), Error = OstreeError>> {
    let url_clone = url.to_string();
    let repo_path_clone = repo_path.to_path_buf();
    let to = delta.to.clone();
    Box::new(
        if let Some(ref from) = delta.from {
            Either::A(pull_commit_async(
                n_retries,
                repo_path.to_path_buf(),
                url_clone.clone(),
                from.clone(),
            ))
        } else {
            Either::B(future::result(Ok(())))
        }
        .and_then(move |_| pull_commit_async(n_retries, repo_path_clone, url_clone, to)),
    )
}

pub fn generate_delta_async(
    repo_path: &Path,
    delta: &Delta,
) -> Box<dyn Future<Item = (), Error = OstreeError>> {
    let mut cmd = Command::new("flatpak");

    unsafe {
        cmd.pre_exec(|| {
            // Setsid in the child to avoid SIGINT on server killing
            // child and breaking the graceful shutdown
            libc::setsid();
            Ok(())
        });
    }

    cmd.arg("build-update-repo")
        .arg("--generate-static-delta-to")
        .arg(delta.to.clone());

    if let Some(ref from) = delta.from {
        cmd.arg("--generate-static-delta-from").arg(from.clone());
    };

    cmd.arg(&repo_path);

    log::info!("Generating delta {}", delta.to_string());
    Box::new(
        cmd.output_async()
            .map_err(|e| {
                OstreeError::ExecFailed("flatpak build-update-repo".to_string(), e.to_string())
            })
            .and_then(|output| result_from_output(output, "flatpak build-update-repo")),
    )
}

pub fn prune_async(repo_path: &Path) -> Box<dyn Future<Item = (), Error = OstreeError>> {
    let mut cmd = Command::new("ostree");

    unsafe {
        cmd.pre_exec(|| {
            // Setsid in the child to avoid SIGINT on server killing
            // child and breaking the graceful shutdown
            libc::setsid();
            Ok(())
        });
    }

    cmd.arg("prune")
        .arg(&format!("--repo={}", repo_path.to_string_lossy()))
        .arg("--keep-younger-than=3 days ago");

    Box::new(
        cmd.output_async()
            .map_err(|e| OstreeError::ExecFailed("ostree prune".to_string(), e.to_string()))
            .and_then(|output| result_from_output(output, "ostree prune")),
    )
}
