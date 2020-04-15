node{
  stage('SCM Checkout'){
  git 'https://github.com/vibo90/bigdata_spark'
  }
  stage('Compile-Package'){
  def mvnHome tool name: 'maven', type: 'maven'
  sh "{$mvnHome}/bin/mvn clean package"
  }
}
