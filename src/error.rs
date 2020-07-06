/*
 * Copyright 2015-2018 Ben Ashford
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

//! Errors and error conversion code for the `rs_es` crate

use std::io::Read;
use thiserror::Error;

/// Error that can occur include IO and parsing errors, as well as specific
/// errors from the ElasticSearch server and logic errors from this library
#[derive(Error, Debug)]
pub enum EsError {
    /// An internal error from this library
    #[error("RS ES Internal Error: {details:?}")]
    EsError { details: String },

    /// An error reported in a JSON response from the ElasticSearch server
    #[error("Elasticsearch Error: {details:?}")]
    EsServerError { details: String },

    /// Miscellaneous error from the HTTP library
    #[error("HTTP Error")]
    HttpError {
        #[from]
        source: reqwest::Error,
    },

    /// Miscellaneous IO error
    #[error("IO Error")]
    IoError {
        #[from]
        source: std::io::Error,
    },

    /// JSON error
    #[error("JSON Error")]
    JsonError {
        #[from]
        source: serde_json::error::Error,
    },

    /// URL error
    #[error("URL Error")]
    URLError {
        #[from]
        source: url::ParseError,
    },
}

impl<'a> From<&'a mut reqwest::blocking::Response> for EsError {
    fn from(err: &'a mut reqwest::blocking::Response) -> EsError {
        let mut buffer = String::new();
        match err.read_to_string(&mut buffer) {
            Ok(_) => EsError::EsServerError {
                details: format!("{} - {}", err.status(), buffer),
            },
            Err(_) => EsError::EsServerError {
                details: format!("{} - cannot read response - {:?}", err.status(), err),
            },
        }
    }
}
