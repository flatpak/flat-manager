pipeline {
    agent {
        docker {
            image 'rust:slim'
            // Need to run as root to install packages
            args '-u 0:0'
        }
    }
    stages {
        stage('Dependencies') {
            steps {
                sh 'apt-get update'
                sh 'apt-get -y install libpq-dev'
                sh 'cargo fetch'
            }
        }
        stage('Build') {
            steps {
                sh 'cargo build --release'
            }
        }
        stage('Test') {
            steps {
                sh 'cargo test --release'
            }
        }
        stage('Publish') {
            steps {
                archiveArtifacts 'flat-manager-client'
                archiveArtifacts 'target/release/delta-generator-client'
                archiveArtifacts 'target/release/flat-manager'
                archiveArtifacts 'target/release/gentoken'
            }
        }
    }
}
