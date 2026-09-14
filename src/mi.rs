use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq)]
pub enum MiValue {
    Const(String),
    Tuple(BTreeMap<String, MiValue>),
    List(Vec<MiValue>),
}

impl MiValue {
    pub fn text(&self) -> Option<&str> {
        match self {
            Self::Const(value) => Some(value),
            _ => None,
        }
    }

    pub fn field(&self, name: &str) -> Option<&MiValue> {
        match self {
            Self::Tuple(fields) => fields.get(name),
            _ => None,
        }
    }

    pub fn items(&self) -> &[MiValue] {
        match self {
            Self::List(items) => items,
            _ => &[],
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum MiRecord {
    Result {
        token: Option<u64>,
        class: String,
        results: BTreeMap<String, MiValue>,
    },
    Async {
        kind: char,
        class: String,
        results: BTreeMap<String, MiValue>,
    },
    Stream {
        kind: char,
        text: String,
    },
    Prompt,
}

pub fn parse_line(line: &str) -> Result<MiRecord, String> {
    let line = line.trim_end_matches(['\r', '\n']);
    if line == "(gdb)" || line == "(gdb) " {
        return Ok(MiRecord::Prompt);
    }
    let mut parser = Parser::new(line);
    let token = parser.number();
    let marker = parser.next().ok_or_else(|| "empty MI record".to_string())?;
    match marker {
        '^' => {
            let class = parser.word();
            let results = parser.results()?;
            Ok(MiRecord::Result {
                token,
                class,
                results,
            })
        }
        '*' | '+' | '=' => {
            let class = parser.word();
            let results = parser.results()?;
            Ok(MiRecord::Async {
                kind: marker,
                class,
                results,
            })
        }
        '~' | '@' | '&' => Ok(MiRecord::Stream {
            kind: marker,
            text: parser.string()?,
        }),
        _ => Err(format!("unsupported MI marker {marker:?}")),
    }
}

struct Parser<'a> {
    input: &'a [u8],
    pos: usize,
}

impl<'a> Parser<'a> {
    fn new(input: &'a str) -> Self {
        Self {
            input: input.as_bytes(),
            pos: 0,
        }
    }
    fn peek(&self) -> Option<char> {
        self.input.get(self.pos).map(|b| *b as char)
    }
    fn next(&mut self) -> Option<char> {
        let value = self.peek()?;
        self.pos += 1;
        Some(value)
    }

    fn number(&mut self) -> Option<u64> {
        let start = self.pos;
        while self.peek().is_some_and(|c| c.is_ascii_digit()) {
            self.pos += 1;
        }
        (self.pos > start)
            .then(|| {
                std::str::from_utf8(&self.input[start..self.pos])
                    .ok()?
                    .parse()
                    .ok()
            })
            .flatten()
    }

    fn word(&mut self) -> String {
        let start = self.pos;
        while self
            .peek()
            .is_some_and(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_'))
        {
            self.pos += 1;
        }
        String::from_utf8_lossy(&self.input[start..self.pos]).into_owned()
    }

    fn results(&mut self) -> Result<BTreeMap<String, MiValue>, String> {
        let mut fields = BTreeMap::new();
        while self.peek() == Some(',') {
            self.pos += 1;
            let (key, value) = self.result()?;
            if let Some(existing) = fields.remove(&key) {
                let values = match existing {
                    MiValue::List(v) => v,
                    other => vec![other],
                };
                let mut values = values;
                values.push(value);
                fields.insert(key, MiValue::List(values));
            } else {
                fields.insert(key, value);
            }
        }
        Ok(fields)
    }

    fn result(&mut self) -> Result<(String, MiValue), String> {
        let key = self.word();
        if key.is_empty() || self.next() != Some('=') {
            return Err("invalid MI result".into());
        }
        Ok((key, self.value()?))
    }

    fn value(&mut self) -> Result<MiValue, String> {
        match self.peek() {
            Some('"') => Ok(MiValue::Const(self.string()?)),
            Some('{') => {
                self.pos += 1;
                let mut fields = BTreeMap::new();
                if self.peek() != Some('}') {
                    loop {
                        let (key, value) = self.result()?;
                        fields.insert(key, value);
                        if self.peek() != Some(',') {
                            break;
                        }
                        self.pos += 1;
                    }
                }
                if self.next() != Some('}') {
                    return Err("unterminated tuple".into());
                }
                Ok(MiValue::Tuple(fields))
            }
            Some('[') => {
                self.pos += 1;
                let mut items = Vec::new();
                if self.peek() != Some(']') {
                    loop {
                        let saved = self.pos;
                        let key = self.word();
                        if !key.is_empty() && self.peek() == Some('=') {
                            self.pos += 1;
                            let mut fields = BTreeMap::new();
                            fields.insert(key, self.value()?);
                            items.push(MiValue::Tuple(fields));
                        } else {
                            self.pos = saved;
                            items.push(self.value()?);
                        }
                        if self.peek() != Some(',') {
                            break;
                        }
                        self.pos += 1;
                    }
                }
                if self.next() != Some(']') {
                    return Err("unterminated list".into());
                }
                Ok(MiValue::List(items))
            }
            _ => {
                let start = self.pos;
                while self.peek().is_some_and(|c| !matches!(c, ',' | ']' | '}')) {
                    self.pos += 1;
                }
                Ok(MiValue::Const(
                    String::from_utf8_lossy(&self.input[start..self.pos]).into_owned(),
                ))
            }
        }
    }

    fn string(&mut self) -> Result<String, String> {
        if self.next() != Some('"') {
            return Err("expected MI string".into());
        }
        let mut output = String::new();
        loop {
            match self.next() {
                Some('"') => return Ok(output),
                Some('\\') => match self.next().ok_or_else(|| "truncated escape".to_string())? {
                    'n' => output.push('\n'),
                    'r' => output.push('\r'),
                    't' => output.push('\t'),
                    '"' => output.push('"'),
                    '\\' => output.push('\\'),
                    first @ '0'..='7' => {
                        let mut value = first.to_digit(8).unwrap();
                        for _ in 0..2 {
                            if let Some(next @ '0'..='7') = self.peek() {
                                self.pos += 1;
                                value = value * 8 + next.to_digit(8).unwrap();
                            } else {
                                break;
                            }
                        }
                        output.push(char::from_u32(value).unwrap_or('\u{fffd}'));
                    }
                    other => output.push(other),
                },
                Some(c) => output.push(c),
                None => return Err("unterminated MI string".into()),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_stop_record() {
        let record = parse_line(
            r#"*stopped,reason="breakpoint-hit",frame={addr="0x401000",func="main",line="7"}"#,
        )
        .unwrap();
        let MiRecord::Async { class, results, .. } = record else {
            panic!()
        };
        assert_eq!(class, "stopped");
        assert_eq!(results["reason"].text(), Some("breakpoint-hit"));
        assert_eq!(
            results["frame"].field("func").and_then(MiValue::text),
            Some("main")
        );
    }

    #[test]
    fn parses_nested_lists_and_escapes() {
        let record = parse_line(
            r#"31^done,stack=[frame={level="0",func="vuln\\n"},frame={level="1",func="main"}]"#,
        )
        .unwrap();
        let MiRecord::Result { token, results, .. } = record else {
            panic!()
        };
        assert_eq!(token, Some(31));
        assert_eq!(results["stack"].items().len(), 2);
    }
}
