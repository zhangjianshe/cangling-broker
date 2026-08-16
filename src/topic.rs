/// MQTT 3.1.1 topic-filter matching (`+` one level, `#` remaining levels).
pub fn filter_matches(filter: &str, topic: &str) -> bool {
    if filter.is_empty() || topic.is_empty() {
        return false;
    }
    if topic.starts_with('$') && wildcard_at_root(filter) {
        return false;
    }
    let filters: Vec<&str> = filter.split('/').collect();
    let topics: Vec<&str> = topic.split('/').collect();
    let mut fi = 0;
    let mut ti = 0;
    while fi < filters.len() {
        if filters[fi] == "#" {
            return fi + 1 == filters.len();
        }
        if ti >= topics.len() {
            return false;
        }
        if filters[fi] != "+" && filters[fi] != topics[ti] {
            return false;
        }
        fi += 1;
        ti += 1;
    }
    ti == topics.len()
}

pub fn is_valid_publish_topic(topic: &str) -> bool {
    !topic.is_empty() && !topic.contains('\0') && !topic.contains('+') && !topic.contains('#')
}

pub fn is_valid_subscribe_filter(filter: &str) -> bool {
    if filter.is_empty() || filter.contains('\0') {
        return false;
    }
    let parts: Vec<&str> = filter.split('/').collect();
    for (index, part) in parts.iter().enumerate() {
        if *part == "#" {
            return index + 1 == parts.len();
        }
        if *part == "+" {
            continue;
        }
        if part.contains('#') || part.contains('+') {
            return false;
        }
    }
    true
}

pub fn is_wildcard_filter(filter: &str) -> bool {
    filter.contains('+') || filter.contains('#')
}

/// `Some("")` for `#`, `Some("a/b")` for `a/b/#`. `None` if this is not a trailing-`#` filter.
pub fn multi_level_prefix(filter: &str) -> Option<&str> {
    if filter == "#" {
        return Some("");
    }
    filter.strip_suffix("/#").filter(|prefix| {
        !prefix.is_empty() && !is_wildcard_filter(prefix) && is_valid_subscribe_filter(filter)
    })
}

fn wildcard_at_root(filter: &str) -> bool {
    filter == "#" || filter == "+" || filter.starts_with("+/") || filter.starts_with("#")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_matches_this_level_and_children() {
        assert!(filter_matches("sensor/#", "sensor"));
        assert!(filter_matches("sensor/#", "sensor/temp"));
        assert!(filter_matches("sensor/#", "sensor/a/b"));
        assert!(!filter_matches("sensor/#", "sensors"));
        assert!(!filter_matches("sensor/#", "other/temp"));
        assert!(filter_matches("/ibuser/1/#", "/ibuser/1/dRueErAe"));
        assert!(filter_matches("/ibuser/1/#", "/ibuser/1"));
        assert!(!filter_matches("/ibuser/1/#", "/ibuser/2/dRueErAe"));
        assert!(filter_matches("#", "a"));
        assert!(filter_matches("#", "a/b/c"));
    }

    #[test]
    fn hash_does_not_match_dollar_topics() {
        assert!(!filter_matches("#", "$SYS/load"));
        assert!(!filter_matches("+/load", "$SYS/load"));
        assert!(filter_matches("$SYS/#", "$SYS/load"));
    }

    #[test]
    fn plus_matches_one_level() {
        assert!(filter_matches("sensor/+/temp", "sensor/x/temp"));
        assert!(!filter_matches("sensor/+/temp", "sensor/x/y/temp"));
        assert!(!filter_matches("sensor/+/temp", "sensor/temp"));
    }

    #[test]
    fn rejects_malformed_hash_filters() {
        assert!(!is_valid_subscribe_filter(""));
        assert!(!is_valid_subscribe_filter("sensor#"));
        assert!(!is_valid_subscribe_filter("a/#/b"));
        assert!(!is_valid_subscribe_filter("#/a"));
        assert!(is_valid_subscribe_filter("#"));
        assert!(is_valid_subscribe_filter("sensor/#"));
        assert!(is_valid_subscribe_filter("sensor/+/temp"));
    }

    #[test]
    fn publish_topics_cannot_contain_wildcards() {
        assert!(is_valid_publish_topic("sensor/temp"));
        assert!(!is_valid_publish_topic("sensor/#"));
        assert!(!is_valid_publish_topic("sensor/+/temp"));
        assert!(!is_valid_publish_topic(""));
    }

    #[test]
    fn hash_prefix_extraction() {
        assert_eq!(multi_level_prefix("#"), Some(""));
        assert_eq!(multi_level_prefix("sensor/#"), Some("sensor"));
        assert_eq!(multi_level_prefix("a/b/#"), Some("a/b"));
        assert_eq!(multi_level_prefix("sensor/+/x/#"), None);
        assert_eq!(multi_level_prefix("sensor"), None);
    }
}
