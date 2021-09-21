use anyhow::{anyhow, Result};
use regex::Regex;
use std::io::ErrorKind;
use std::process::Command;
use std::thread;
use std::time;
use subprocess::{Exec, Redirection};
use tracing::debug;
use tracing::error;
use tracing::info;

/// Runs a command and returns the output as a string.
///
/// Both stderr and stdout are returned in the result.
///
/// # Arguments
/// * `command` - The system command to run
/// * `args` - An array of command line arguments for the command
///
fn run_command(command: &str, args: &[&str]) -> Result<String> {
    debug!("executing {}", command);
    let data = Exec::cmd(command)
        .args(args)
        .stdout(Redirection::Pipe)
        .stderr(Redirection::Merge)
        .capture()?;

    if data.exit_status.success() {
        Ok(data.stdout_str().to_string())
    } else {
        Err(anyhow!(
            "command {} {:?} exited with {:?} and output:\n{}",
            command,
            args,
            data.exit_status,
            data.stdout_str()
        ))
    }
}

pub struct DockerCompose {
    file_path: String,
}

impl DockerCompose {
    /// Creates a new DockerCompose object by submitting a file to the underlying docker-compose
    /// system.  Executes `docker-compose -f [file_path] up -d`
    ///
    /// # Notes:
    /// * Does not sleep - Calling processes should sleep or use `wait_for()` to delay until the
    /// containers are ready.
    ///
    /// # Arguments
    /// * `file_path` - The path to the docker-compose yaml file.
    ///
    /// # Panics
    /// * Will panic if docker-compose is not installed
    ///
    pub fn new(file_path: &str) -> Self {
        if let Err(ErrorKind::NotFound) = Command::new("docker-compose")
            .output()
            .map_err(|e| e.kind())
        {
            panic!("Could not find docker-compose. Have you installed it?");
        }

        DockerCompose::clean_up(file_path).unwrap();

        let result = run_command("docker-compose", &["-f", file_path, "up", "-d"]).unwrap();
        info!("Starting {}: {}", file_path, result);

        DockerCompose {
            file_path: file_path.to_string(),
        }
    }

    /// Waits for a string to appear in the docker-compose log output.
    ///
    /// Uses `regex.is_match()` to locate the match.
    ///
    /// This is shorthand for `wait_for_n( log_text, 1 )`
    ///
    /// # Arguments
    /// * `log_text` - A regular expression defining the text to find in the docker-container log
    /// output.
    ///
    /// # Panics
    /// * If `log_text` is not found within 60 seconds.
    ///
    /// # Example
    /// ```
    /// let _compose = DockerCompose::new("examples/redis-passthrough/docker-compose.yml")
    ///         .wait_for("Ready to accept connections");
    /// ```
    ///
    pub fn wait_for(self, log_text: &str) -> Self {
        self.wait_for_n(log_text, 1)
    }

    /// Waits for a string to appear in the docker-compose log output `count` times.
    ///
    /// Counts the number of items returned by `regex.find_iter`.
    ///
    /// # Arguments
    /// * `log_text` - A regular expression defining the text to find in the docker-container log
    /// output.
    /// * `count` - The number of times the regular expression should be found.
    ///
    /// # Panics
    /// * If `count` occurances of `log_text` is not found in the log within 60 seconds.
    ///
    /// # Example
    /// ```
    /// let _compose = DockerCompose::new("examples/redis-passthrough/docker-compose.yml")
    ///         .wait_for("Ready to accept connections");
    /// ```
    ///
    pub fn wait_for_n(self, log_text: &str, count: usize) -> Self {
        info!("wait_for_n: '{}' {}", log_text, count);
        let args = ["-f", &self.file_path, "logs"];
        let re = Regex::new(log_text).unwrap();
        let sys_time = time::SystemTime::now();
        let mut result = run_command("docker-compose", &args).unwrap();
        while re.find_iter(&result).count() < count {
            match sys_time.elapsed() {
                Ok(elapsed) => {
                    if elapsed.as_secs() > 60 {
                        debug!("{}", result);
                        panic!("wait_for: Timer expired");
                    }
                }
                Err(e) => {
                    // an error occurred!
                    info!("Clock aberration: {:?}", e);
                }
            }
            debug!("wait_for_n: looping");
            result = run_command("docker-compose", &args).unwrap();
        }
        info!("wait_for_n: found '{}' {} times", log_text, count);
        self
    }

    /// Cleans up the docker-compose by shutting down the running system and removing the images.
    ///
    /// # Arguments
    /// * `file_path` - The path to the docker-compose yaml file that was used to start docker.
    fn clean_up(file_path: &str) -> Result<()> {
        info!("bringing down docker compose {}", file_path);

        run_command("docker-compose", &["-f", file_path, "down", "-v"])?;
        run_command("docker-compose", &["-f", file_path, "rm", "-f", "-s", "-v"])?;

        thread::sleep(time::Duration::from_secs(1));

        Ok(())
    }
}

impl Drop for DockerCompose {
    fn drop(&mut self) {
        if std::thread::panicking() {
            if let Err(err) = DockerCompose::clean_up(&self.file_path) {
                println!(
                    "ERROR: docker compose failed to bring down while already panicking: {:?}",
                    err
                );
                error!(
                    "docker compose failed to bring down while already panicking: {:?}",
                    err
                );
            }
        } else {
            DockerCompose::clean_up(&self.file_path).unwrap();
        }
    }
}