assemblyMergeStrategy in assembly := {
  case PathList("org", "apache", "spark", "unused", "UnusedStubClass.class") => MergeStrategy.first
  case PathList("lib") => MergeStrategy.first
//  case x => (assemblyMergeStrategy in assembly).value(x)
  case _ => MergeStrategy.first
}