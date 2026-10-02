import sbt._
import sbt.Keys._

/** Maven Central (Sonatype Central Portal) publishing, switched on by `PUBLISH_TARGET=central`. Without it the build
  * publishes to the platform Nexus exactly as before (build.sbt), so nothing here affects CI or local work.
  *
  * With it: releases are staged under `target/sona-staging` for sbt's built-in `sonaUpload` / `sonaRelease`
  * (credentials from `SONATYPE_USERNAME` / `SONATYPE_PASSWORD`, a Central Portal user token), snapshots go to the
  * Central snapshots repository, and every module carries what Central validates: a javadoc jar, a POM free of private
  * repositories, and (via sbt-pgp's `publishSigned`) signatures. Copied from teob's build; see
  * `.github/workflows/snapshot.yml` and `.github/workflows/release.yml`.
  */
object CentralPublishing extends AutoPlugin {
  override def trigger  = allRequirements
  override def requires = plugins.JvmPlugin

  val toCentral: Boolean = sys.env.get("PUBLISH_TARGET").contains("central")

  private val centralHost = "central.sonatype.com"

  override def buildSettings: Seq[Setting[_]] =
    if (!toCentral) Nil
    else
      Seq(
        credentials ++= (for {
          user <- sys.env.get("SONATYPE_USERNAME")
          pass <- sys.env.get("SONATYPE_PASSWORD")
        } yield Credentials("central-snapshots", centralHost, user, pass)).toSeq
      )

  override def projectSettings: Seq[Setting[_]] =
    if (!toCentral) Nil
    else
      Seq(
        publishTo := {
          if (isSnapshot.value) Some("central-snapshots" at s"https://$centralHost/repository/maven-snapshots/")
          else localStaging.value
        },
        // Central requires a javadoc jar; ship it without pages rather than make a release wait on scaladoc.
        Compile / packageDoc / publishArtifact := true,
        // Test delegates to Compile for this key, and test jars are not published at all.
        Test / packageDoc / publishArtifact := false,
        Compile / doc / sources := Seq.empty,
        // Keep the platform Nexus (a private, allow-listed host) out of published POMs.
        pomIncludeRepository := (_ => false)
      )
}
