def gitBranch = env.BRANCH_NAME
def gitURL = "git@github.com:Memphisdev/memphis.go.git"
def repoUrlPrefix = "memphisos"

node ("small-ec2-fleet") {
  git credentialsId: 'main-github', url: gitURL, branch: gitBranch
  
  try{
    
   stage('Deploy GO SDK') {

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
