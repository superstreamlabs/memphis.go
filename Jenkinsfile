def gitBranch = env.BRANCH_NAME
def gitURL = "git@github.com:Memphisdev/memphis.go.git"
def repoUrlPrefix = "memphisos"

node ("small-ec2-fleet") {
  git credentialsId: 'main-github', url: gitURL, branch: gitBranch
  def versionTag = readFile "./version.conf"
  
  try{
    stage ('Install GoLang') {
      wget https://go.dev/dl/go1.18.4.linux-amd64.tar.gz
      sudo  tar -C /usr/local -xzf go1.18.4.linux-amd64.tar.gz
      sudo ln -s /usr/local/go/bin/go /usr/bin/go
    }
    
    stage('Deploy GO SDK') {
      sh 'git tag v${versionTag}'
      sh 'git push origin v${versionTag}'
      sh 'GOPROXY=proxy.golang.org go list -m github.com/memphisdev/memphis.go@v${versionTag}'
    }
    
    notifySuccessful()

  } catch (e) {
      currentBuild.result = "FAILED"
      cleanWs()
      notifyFailed()
      throw e
  }
}

def notifySuccessful() {
  emailext (
      subject: "SUCCESSFUL: Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]'",
      body: """<p>SUCCESSFUL: Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]':</p>
        <p>Check console output at &QUOT;<a href='${env.BUILD_URL}'>${env.JOB_NAME} [${env.BUILD_NUMBER}]</a>&QUOT;</p>""",
      recipientProviders: [[$class: 'DevelopersRecipientProvider']]
    )
}

def notifyFailed() {
  emailext (
      subject: "FAILED: Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]'",
      body: """<p>FAILED: Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]':</p>
        <p>Check console output at &QUOT;<a href='${env.BUILD_URL}'>${env.JOB_NAME} [${env.BUILD_NUMBER}]</a>&QUOT;</p>""",
      recipientProviders: [[$class: 'DevelopersRecipientProvider']]
    )
}