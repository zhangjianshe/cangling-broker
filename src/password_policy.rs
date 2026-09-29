use fancy_regex::Regex;
use rand::{seq::SliceRandom, thread_rng, Rng};

const DEFAULT_REGEX: &str = r"^(?=.{8,128}$)(?=.*[a-z])(?=.*[A-Z])(?=.*[^A-Za-z0-9\s]).*$";
const DEFAULT_HINT: &str =
    "密码必须为 8–128 位，并至少包含一个大写字母、一个小写字母和一个特殊字符";

pub struct PasswordPolicy {
    regex: Regex,
    hint: String,
    generated_length: usize,
}

impl PasswordPolicy {
    pub fn from_env() -> Result<Self, String> {
        let pattern =
            std::env::var("CL_BROKER_PASSWORD_REGEX").unwrap_or_else(|_| DEFAULT_REGEX.to_owned());
        let hint =
            std::env::var("CL_BROKER_PASSWORD_HINT").unwrap_or_else(|_| DEFAULT_HINT.to_owned());
        let generated_length = match std::env::var("CL_BROKER_PASSWORD_GENERATED_LENGTH") {
            Ok(value) => value
                .parse::<usize>()
                .map_err(|_| "CL_BROKER_PASSWORD_GENERATED_LENGTH 必须是整数".to_owned())?,
            Err(_) => 8,
        };
        if !(8..=128).contains(&generated_length) {
            return Err("CL_BROKER_PASSWORD_GENERATED_LENGTH 必须在 8–128 之间".to_owned());
        }
        Ok(Self {
            regex: Regex::new(&pattern)
                .map_err(|error| format!("CL_BROKER_PASSWORD_REGEX 无效：{error}"))?,
            hint,
            generated_length,
        })
    }

    pub fn validate(&self, password: &str) -> Result<(), String> {
        match self.regex.is_match(password) {
            Ok(true) => Ok(()),
            Ok(false) => Err(self.hint.clone()),
            Err(error) => Err(format!("密码规则匹配失败：{error}")),
        }
    }

    pub fn generate(&self) -> Result<String, String> {
        const LOWER: &[u8] = b"abcdefghijkmnopqrstuvwxyz";
        const UPPER: &[u8] = b"ABCDEFGHJKLMNPQRSTUVWXYZ";
        const DIGITS: &[u8] = b"23456789";
        const SPECIAL: &[u8] = b"!@#$%^&*_-+=";
        let all = [LOWER, UPPER, DIGITS, SPECIAL].concat();
        let mut rng = thread_rng();
        for _ in 0..1024 {
            let mut bytes = vec![
                *LOWER.choose(&mut rng).unwrap(),
                *UPPER.choose(&mut rng).unwrap(),
                *DIGITS.choose(&mut rng).unwrap(),
                *SPECIAL.choose(&mut rng).unwrap(),
            ];
            bytes.extend(
                (bytes.len()..self.generated_length).map(|_| all[rng.gen_range(0..all.len())]),
            );
            bytes.shuffle(&mut rng);
            let password = String::from_utf8(bytes).unwrap();
            if self.validate(&password).is_ok() {
                return Ok(password);
            }
        }
        Err("无法生成满足密码规则的密码；请通过 -p 指定密码".to_owned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_policy_and_generator_work() {
        let policy = PasswordPolicy::from_env().unwrap();
        assert!(policy.validate("-Cangling@zky").is_ok());
        assert!(policy.validate("lowercase!").is_err());
        assert!(policy.validate(&policy.generate().unwrap()).is_ok());
    }
}
