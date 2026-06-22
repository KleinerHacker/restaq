import com.github.jk1.license.render.ReportRenderer

plugins {
	kotlin("jvm") version "2.3.21"
	kotlin("plugin.spring") version "2.3.21"
	id("org.springframework.boot") version "4.1.0"
	id("io.spring.dependency-management") version "1.1.7"
	id("org.jetbrains.kotlinx.kover") version "0.9.1"
	id("org.jetbrains.dokka") version "2.2.0"
	id("com.github.jk1.dependency-license-report") version "2.5"
	// Pinned to 3.2.2: 3.2.3+ regressed on Gradle 9 — io.spring.dependency-management observes
	// the `cyclonedxDirectBom` configuration as a variant before the plugin registers its artifacts,
	// which Gradle 9 then rejects ("Cannot mutate ... consumed as a variant"). See cyclonedx #821.
	id("org.cyclonedx.bom") version "3.2.2"
	id("app.cash.licensee") version "1.14.1"
}

group = "org.pcsoft.micro"
version = "0.0.1-SNAPSHOT"

java {
	toolchain {
		languageVersion = JavaLanguageVersion.of(25)
	}
}

repositories {
	mavenCentral()
}

dependencies {
	implementation("org.springframework.boot:spring-boot-starter")
	implementation("org.springframework.boot:spring-boot-starter-webflux")
	implementation("org.springframework.boot:spring-boot-starter-amqp")
	implementation("org.springframework.boot:spring-boot-starter-artemis")
	implementation("org.jetbrains.kotlin:kotlin-reflect")
	testImplementation("org.springframework.boot:spring-boot-starter-test")
	testImplementation("org.jetbrains.kotlin:kotlin-test-junit5")
	testImplementation("org.mockito.kotlin:mockito-kotlin:5.4.0")
	testImplementation("org.powermock:powermock-reflect:2.0.9")
	testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

kotlin {
	compilerOptions {
		freeCompilerArgs.addAll("-Xjsr305=strict", "-Xannotation-default-target=param-property")
	}
}

kover {
	reports {
		filters {
			excludes {
				// Declarations annotated @Generated are intentionally uncoverable
				// (defensive / provably-unreachable code).
				annotatedBy("org.pcsoft.intellij.plugin.inno_setup.Generated")
			}
		}
	}
}

licenseReport {
	outputDir = layout.buildDirectory.dir("licences").get().asFile.absolutePath

	configurations = arrayOf("runtimeClasspath")

	renderers = arrayOf<ReportRenderer>(
		com.github.jk1.license.render.JsonReportRenderer(),
		com.github.jk1.license.render.SimpleHtmlReportRenderer()
	)
}

plugins.withId("org.jetbrains.kotlin.jvm") {
	plugins.withId("app.cash.licensee") {
		extensions.configure<app.cash.licensee.LicenseeExtension> {
			listOf(
				"Apache-2.0", "EPL-1.0", "EPL-2.0", "MIT-0"
			).forEach(::allow)

			listOf(
				"https://opensource.org/license/mit" //MIT
			).forEach(::allowUrl)
		}
	}
}

tasks.withType<Test> {
	useJUnitPlatform()
}

tasks {
	//region Dokka
	register<Copy>("copyDokka") {
		group = "dokka"
		description = "Copy all Dokka to MkDocs"
		from(File("build/dokka"))
		into(File("docs/docs/dokka"))
		dependsOn("dokkaGeneratePublicationHtml")
	}

	register<Delete>("deleteDokka") {
		group = "dokka"
		description = "Delete Dokka"
		delete(File("docs/docs/dokka"))
	}
	//endregion

	//region Licencing
	register<Copy>("copyLicenceReport") {
		group = "licencing"
		description = "Copy licence report to MkDocs"
		from(File("build/licences"))
		into(File("docs/docs/licences"))
		dependsOn("generateLicenseReport")
	}

	register<Delete>("deleteLicenceReport") {
		group = "licencing"
		description = "Delete licence report"
		delete(File("docs/docs/licences"))
	}
	//endregion

	//region MkDocs
	// mike spawns `mkdocs` as a subprocess; on Windows the Python Scripts dir
	// (where mkdocs.exe lives) is often not on PATH. Resolve it once and prepend
	// it to PATH for the mike tasks. In CI (setup-python) it is already on PATH.
	val pythonScriptsDir: String? by lazy {
		runCatching {
			providers.exec {
				commandLine("python", "-c", "import sysconfig; print(sysconfig.get_path('scripts'))")
			}.standardOutput.asText.get().trim().ifEmpty { null }
		}.getOrNull()
	}
	fun Exec.withMikePath() {
		pythonScriptsDir?.let { dir ->
			environment("PATH", dir + File.pathSeparator + System.getenv("PATH"))
		}
	}

	register<Exec>("installMkDocs") {
		group = null
		description = "Install mkdocs"
		workingDir = file("docs")
		commandLine("python", "-m", "pip", "install", "--upgrade", "mkdocs")
	}

	register<Exec>("installMkDocsMaterial") {
		group = null
		description = "Install mkdocs-material"
		workingDir = file("docs")
		commandLine("python", "-m", "pip", "install", "--upgrade", "mkdocs-material")
	}

	register<Exec>("installGitHubPages") {
		group = null
		description = "Install ghp-import"
		workingDir = file("docs")
		commandLine("python", "-m", "pip", "install", "--upgrade", "ghp-import")
	}

	register<Exec>("installMike") {
		group = null
		description = "Install mike for versioned docs deployment"
		workingDir = file("docs")
		commandLine("python", "-m", "pip", "install", "--upgrade", "mike")
	}

	register<Exec>("installI18N") {
		group = null
		description = "Install i18n"
		workingDir = file("docs")
		commandLine("python", "-m", "pip", "install", "--upgrade", "mkdocs-static-i18n")
	}

	register("installDocs") {
		group = "MKDocs"
		description = "Install mkdocs and dependencies"
		dependsOn("installMkDocs")
		dependsOn("installMkDocsMaterial")
		dependsOn("installGitHubPages")
		dependsOn("installI18N")
		dependsOn("installMike")
	}

	register<Exec>("runDocs") {
		group = "MKDocs"
		description = "Run mkdocs serve and open browser (no version selector — that only appears on the deployed site)"
		workingDir = file("docs")
		commandLine("python", "-m", "mkdocs", "serve", "-o", "-w", ".", "-w", "./docs")
		dependsOn("installDocs", "copyDokka", "copyLicenceReport")
		finalizedBy("deleteDokka", "deleteLicenceReport")
	}

	register<Exec>("buildDocs") {
		group = "MKDocs"
		description = "Build the mkdocs site into build/docs (per mkdocs.yml site_dir; no serve, no deploy) — usable as a generation test"
		workingDir = file("docs")
		// --strict fails the build on warnings (broken links, missing pages …) so it acts as a test;
		// --clean wipes the previous output first.
		commandLine("python", "-m", "mkdocs", "build", "--clean", "--strict")
		dependsOn("installDocs", "copyDokka", "copyLicenceReport")
		finalizedBy("deleteDokka", "deleteLicenceReport")
	}

	register<Exec>("deployDocs") {
		group = "MKDocs"
		description = "Deploy a versioned docs snapshot via mike. Requires -Pversion=<tag> and a pre-configured git push target."
		workingDir = file("docs")
		val ver = (project.findProperty("version") as String?)
			?: error("Pass -Pversion=<tag> to deployDocs")
		val setLatest = (project.findProperty("setLatest") as String?) != "false"
		val args = buildList {
			add("python"); add("-c"); add("from mike.driver import main; main()"); add("deploy"); add("--push")
			if (setLatest) { add("--update-aliases"); add(ver); add("latest") } else add(ver)
		}
		commandLine(args)
		withMikePath()
		dependsOn("installDocs", "copyDokka", "copyLicenceReport")
		finalizedBy("deleteDokka", "deleteLicenceReport")
	}

	register<Exec>("setDefaultDocs") {
		group = "MKDocs"
		description = "Set the default docs version shown at the root URL via mike (run once after the first release deploy)."
		workingDir = file("docs")
		commandLine("python", "-c", "from mike.driver import main; main()", "set-default", "--push", "latest")
		withMikePath()
		dependsOn("installDocs")
	}
	//endregion
}
