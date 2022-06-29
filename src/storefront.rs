use std::collections::HashMap;
use std::error::Error;

use elementtree::Element;
use serde::Deserialize;

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct AllowList {
    pub allowed: Option<Vec<String>>,
    pub blocked: Vec<String>,
}

impl AllowList {
    pub fn check(&self, string: &str) -> bool {
        if self.blocked.contains(&string.to_string()) {
            false
        } else if let Some(allowed) = &self.allowed {
            allowed.contains(&string.to_string())
        } else {
            true
        }
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct StorefrontInfo {
    /* The token type for the flatpak commit. If not present, the token-type will not be changed from what the client
    uploaded or specified when creating the commit. */
    pub token_type: Option<i32>,

    /* Values to add to the appstream <custom> section. */
    pub add_custom_values: HashMap<String, String>,
    /* URLs to add to the appstream file. */
    pub add_urls: HashMap<String, String>,

    /* Settings for validating uploaded appstream files. These settings don't affect anything added by the
    StorefrontInfo itself. */
    /* Check the XML tags themselves */
    pub check_xml_tags: AllowList,
    /* Check the types of <url/> tags */
    pub check_url_types: AllowList,
    /* Check the keys in the <custom/> section */
    pub check_custom_values: AllowList,
}

impl StorefrontInfo {
    pub fn validate_storefront_info(
        &self,
        expected_appid: &str,
        original_appstream: &str,
    ) -> Result<(), Box<dyn Error>> {
        let root = Element::from_reader(original_appstream.as_bytes())?;

        let components: Vec<_> = root.children().collect();
        if components.len() > 1 {
            return Err(
                "There should only be one <component/> element in the appstream file".into(),
            );
        } else if components.is_empty() {
            return Err("No components found in the appstream file".into());
        } else if components[0].tag().name() != "component" {
            return Err(format!(
                "Expected <component/> element, not <{}/>",
                components[0].tag()
            )
            .into());
        }

        let component = components[0];

        /* Make sure the id tag is correct */
        let id_tags: Vec<_> = component.find_all("id").collect();
        if id_tags.len() > 1 {
            return Err("Duplicate <id/> tag in appstream file".into());
        } else if id_tags.is_empty() {
            return Err("No <id/> tag found in appstream file".into());
        } else if id_tags[0].text() != expected_appid {
            return Err(format!(
                "Expected app ID to be '{}', not '{}'",
                expected_appid,
                id_tags[0].text()
            )
            .into());
        }

        for element in component.children() {
            match element.tag().name() {
                "url" => {
                    /* Filter existing URLs */
                    if let Some(url_type) = element.get_attr("type") {
                        if !self.check_xml_tags.check(url_type) {
                            return Err(format!(
                                "Appstream <url type='{}'/> is not permitted.",
                                url_type
                            )
                            .into());
                        }
                    } else {
                        return Err("Appstream <url/> must have a 'type' attribute.".into());
                    }
                }

                "custom" => {
                    for custom_child in element.children() {
                        if let Some(key) = custom_child.get_attr("key") {
                            if !self.check_custom_values.check(key) {
                                return Err(format!(
                                    "Appstream <value key='{}'/> is not permitted.",
                                    key
                                )
                                .into());
                            }
                        } else {
                            return Err("Appstream <value/> must have a 'key' attribute.".into());
                        }
                    }
                }

                other => {
                    if !self.check_xml_tags.check(other) {
                        return Err(format!("Appstream tag <{}/> is not permitted.", other).into());
                    }
                }
            }
        }

        Ok(())
    }

    pub fn apply_storefront_info(
        &self,
        original_appstream: &str,
    ) -> Result<String, Box<dyn Error>> {
        /* If there's no changes to make, return the original file. This avoids making trivial syntax changes to the
        XML, forcing a re-commit where none is needed. */
        if self.add_custom_values.is_empty() && self.add_urls.is_empty() {
            return Ok(original_appstream.to_string());
        }

        let mut root = Element::from_reader(original_appstream.as_bytes())?;

        let mut components: Vec<_> = root.children_mut().collect();
        let component = &mut components[0];

        fn find_element<'a>(
            parent: &'a mut Element,
            tag: &'a str,
            attr: Option<(&'_ str, &'_ str)>,
        ) -> Option<&'a mut Element> {
            let existing = if let Some((key, val)) = attr {
                parent
                    .find_all_mut(tag)
                    .find(|el| el.get_attr(key) == Some(val))
            } else {
                parent.find_mut(tag)
            };

            existing
        }

        fn find_or_create_element<'a>(
            parent: &'a mut Element,
            tag: &'a str,
            attr: Option<(&'_ str, &'_ str)>,
        ) -> &'a mut Element {
            if find_element(parent, tag, attr).is_some() {
                // running find_element twice is a borrow checker workaround
                find_element(parent, tag, attr).unwrap()
            } else {
                let new_tag = parent.append_new_child(tag);
                if let Some((key, val)) = attr {
                    new_tag.set_attr(key, val);
                }
                new_tag
            }
        }

        fn sort_hash_map(map: &HashMap<String, String>) -> Vec<(&String, &String)> {
            /* Hash maps iterate in an arbitrary order. Sort them so that diffs are less cluttered and so the tests work
            reliably. */
            let mut values = map.iter().collect::<Vec<(&String, &String)>>();
            values.sort();
            values
        }

        for (key, value) in sort_hash_map(&self.add_custom_values) {
            let custom = find_or_create_element(component, "custom", None);
            find_or_create_element(custom, "value", Some(("key", key))).set_text(value);
        }

        for (key, value) in sort_hash_map(&self.add_urls) {
            find_or_create_element(component, "url", Some(("type", key))).set_text(value);
        }

        Ok(root.to_string()?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run_test(input: &str, expected: &str) {
        let mut info = StorefrontInfo::default();
        info.add_custom_values
            .insert("TestKey".to_string(), "TestValue".to_string());
        info.add_custom_values
            .insert("TestKey4".to_string(), "TestValue4".to_string());

        info.add_urls
            .insert("homepage".to_string(), "https://example.com".to_string());
        info.add_urls
            .insert("bugtracker".to_string(), "https://github.com".to_string());

        info.validate_storefront_info("org.flatpak.Test", input)
            .unwrap();

        let output = info.apply_storefront_info(input).unwrap();

        assert_eq!(output, expected);
    }

    #[test]
    fn test_no_changes() {
        const INPUT: &str = r#"<?xml version="1.0" encoding="utf-8"?>
<components>
    <component>
        <id>org.flatpak.Test</id>
    </component>
</components>"#;

        let info = StorefrontInfo::default();
        info.validate_storefront_info("org.flatpak.Test", INPUT)
            .unwrap();
        let output = info.apply_storefront_info(INPUT).unwrap();

        assert_eq!(INPUT, output);
    }

    #[test]
    fn test_add_info() {
        const INPUT: &str = r#"
<components>
    <component>
        <id>org.flatpak.Test</id>
    </component>
</components>"#;
        const EXPECTED: &str = r#"<?xml version="1.0" encoding="utf-8"?><components>
    <component>
        <id>org.flatpak.Test</id>
    <custom><value key="TestKey">TestValue</value><value key="TestKey4">TestValue4</value></custom><url type="bugtracker">https://github.com</url><url type="homepage">https://example.com</url></component>
</components>"#;

        run_test(INPUT, EXPECTED);
    }

    #[test]
    fn test_replace_info() {
        const INPUT: &str = r#"
<components>
    <component>
        <id>org.flatpak.Test</id>
        <url type="homepage">https://flatpak.org</url>
        <custom>
            <value key="TestKey1">TestValue1</value>
            <value key="TestKey">TestValue2</value>
            <value key="TestKey2">TestValue3</value>
        </custom>
    </component>
</components>"#;
        const EXPECTED: &str = r#"<?xml version="1.0" encoding="utf-8"?><components>
    <component>
        <id>org.flatpak.Test</id>
        <url type="homepage">https://example.com</url>
        <custom>
            <value key="TestKey1">TestValue1</value>
            <value key="TestKey">TestValue</value>
            <value key="TestKey2">TestValue3</value>
        <value key="TestKey4">TestValue4</value></custom>
    <url type="bugtracker">https://github.com</url></component>
</components>"#;

        run_test(INPUT, EXPECTED);
    }

    fn run_error_test(input: &str, expected_error: &str) {
        let mut info = StorefrontInfo::default();

        info.check_url_types.blocked = vec!["bugtracker".to_string()];
        info.check_custom_values.blocked = vec!["TestKey2".to_string()];

        info.check_xml_tags.allowed = Some(vec![
            "id".to_string(),
            "url".to_string(),
            "custom".to_string(),
            "tags".to_string(),
        ]);

        let err = info
            .validate_storefront_info("org.flatpak.Test", input)
            .unwrap_err();

        assert_eq!(err.to_string(), expected_error);
    }

    #[test]
    fn test_bad_app_id() {
        const INPUT: &str = r#"
<components>
    <component>
        <id>org.flatpak.NotTest</id>
    </component>
</components>
        "#;
        run_error_test(
            INPUT,
            "Expected app ID to be 'org.flatpak.Test', not 'org.flatpak.NotTest'",
        );
    }

    #[test]
    fn test_blocked_url_type() {
        const INPUT: &str = r#"
<components>
    <component>
        <id>org.flatpak.Test</id>
        <url type="bugtracker">https://flatpak.org</url>
    </component>
</components>
        "#;
        run_error_test(
            INPUT,
            "Appstream <url type='bugtracker'/> is not permitted.",
        );
    }

    #[test]
    fn test_blocked_custom_value() {
        const INPUT: &str = r#"
<components>
    <component>
        <id>org.flatpak.Test</id>
        <custom>
            <value key="TestKey2">TestValue</value>
        </custom>
    </component>
</components>
        "#;
        run_error_test(INPUT, "Appstream <value key='TestKey2'/> is not permitted.");
    }

    #[test]
    fn test_blocked_xml_tag() {
        const INPUT: &str = r#"
<components>
    <component>
        <id>org.flatpak.Test</id>
        <description></description>
    </component>
</components>
        "#;
        run_error_test(INPUT, "Appstream tag <description/> is not permitted.");
    }

    #[test]
    fn test_url_no_type() {
        const INPUT: &str = r#"
<components>
    <component>
        <id>org.flatpak.Test</id>
        <url>https://flatpak.org</url>
    </component>
</components>
        "#;
        run_error_test(INPUT, "Appstream <url/> must have a 'type' attribute.");
    }

    #[test]
    fn test_value_no_key() {
        const INPUT: &str = r#"
<components>
    <component>
        <id>org.flatpak.Test</id>
        <custom>
            <value>value</value>
        </custom>
    </component>
</components>
        "#;
        run_error_test(INPUT, "Appstream <value/> must have a 'key' attribute.");
    }
}
