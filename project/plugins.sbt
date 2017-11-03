resolvers += Resolver.url("scoverage-bintray", url("https://dl.bintray.com/sksamuel/sbt-plugins/"))(Resolver.ivyStylePatterns)
addSbtPlugin("org.scoverage" %% "sbt-scoverage" % "1.5.1")

addSbtPlugin("org.scalariform" % "sbt-scalariform" % "1.8.1")
