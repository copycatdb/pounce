use crate::errors::to_pyerr;
use crate::runtime;
use pyo3::prelude::*;
use std::sync::{Arc, Mutex};
use tabby::{AuthMethod, Client, Config, EncryptionLevel};
use tokio::net::TcpStream;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};

pub type SharedClient = Arc<Mutex<Client<Compat<TcpStream>>>>;

pub struct TdsConnection {
    pub client: Option<SharedClient>,
    pub autocommit: bool,
    pub in_transaction: bool,
}

fn parse_connection_string(conn_str: &str) -> (String, u16, String, String, String, bool) {
    let mut host = "localhost".to_string();
    let mut port: u16 = 1433;
    let mut database = "master".to_string();
    let mut uid = String::new();
    let mut pwd = String::new();
    let mut trust_cert = false;

    for part in conn_str.split(';') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        if let Some(idx) = part.find('=') {
            let key = part[..idx].trim().to_lowercase();
            let val = part[idx + 1..].trim().to_string();
            match key.as_str() {
                "server" => {
                    if let Some(comma) = val.find(',') {
                        host = val[..comma].to_string();
                        if let Ok(p) = val[comma + 1..].trim().parse() {
                            port = p;
                        }
                    } else {
                        host = val;
                    }
                }
                "database" | "initial catalog" => database = val,
                "uid" | "user id" => uid = val,
                "pwd" | "password" => pwd = val,
                "trustservercertificate" => {
                    trust_cert = val.eq_ignore_ascii_case("yes")
                        || val == "1"
                        || val.eq_ignore_ascii_case("true");
                }
                _ => {}
            }
        }
    }
    (host, port, database, uid, pwd, trust_cert)
}

#[allow(clippy::await_holding_lock)]
impl TdsConnection {
    pub fn new(connection_str: &str) -> PyResult<Self> {
        let (host, port, database, uid, pwd, trust_cert) = parse_connection_string(connection_str);

        let client = Python::attach(|py| {
            py.detach(|| {
                runtime::block_on(async {
                    let mut config = Config::new();
                    config.host(&host);
                    config.port(port);
                    config.database(&database);
                    config.authentication(AuthMethod::sql_server(&uid, &pwd));
                    if trust_cert {
                        config.trust_cert();
                    }
                    config.encryption(EncryptionLevel::Required);

                    let tcp = TcpStream::connect(config.get_addr()).await.map_err(|e| {
                        pyo3::exceptions::PyConnectionError::new_err(format!(
                            "TCP connect failed: {}",
                            e
                        ))
                    })?;
                    tcp.set_nodelay(true).map_err(|e| {
                        pyo3::exceptions::PyConnectionError::new_err(format!("{}", e))
                    })?;

                    let client =
                        Client::connect(config, tcp.compat_write())
                            .await
                            .map_err(|e| {
                                pyo3::exceptions::PyConnectionError::new_err(format!(
                                    "TDS connect failed: {}",
                                    e
                                ))
                            })?;

                    Ok::<_, PyErr>(client)
                })
            })
        })?;

        Ok(TdsConnection {
            client: Some(Arc::new(Mutex::new(client))),
            autocommit: true,
            in_transaction: false,
        })
    }

    pub fn get_client(&self) -> PyResult<SharedClient> {
        self.client
            .clone()
            .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("Connection is closed"))
    }

    pub fn exec_simple(&self, sql: &str) -> PyResult<()> {
        let client = self.get_client()?;
        let sql = sql.to_string();
        Python::attach(|py| {
            py.detach(|| {
                runtime::block_on(async {
                    let mut c = client.lock().unwrap();
                    c.execute_raw(sql)
                        .await
                        .map_err(to_pyerr)?
                        .into_results()
                        .await
                        .map_err(to_pyerr)?;
                    Ok(())
                })
            })
        })
    }

    pub fn begin_if_needed(&mut self) -> PyResult<()> {
        if !self.autocommit && !self.in_transaction {
            self.exec_simple("BEGIN TRANSACTION")?;
            self.in_transaction = true;
        }
        Ok(())
    }

    pub fn commit(&mut self) -> PyResult<()> {
        if self.in_transaction {
            self.exec_simple("IF @@TRANCOUNT > 0 COMMIT TRANSACTION")?;
            self.in_transaction = false;
        }
        Ok(())
    }

    pub fn rollback(&mut self) -> PyResult<()> {
        if self.in_transaction {
            let _ = self.exec_simple("IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION");
            self.in_transaction = false;
        }
        Ok(())
    }

    pub fn close(&mut self) -> PyResult<()> {
        if self.in_transaction {
            let _ = self.rollback();
        }
        self.client = None;
        Ok(())
    }
}
