# frozen_string_literal: true

require_relative "lib/fb/version"

Gem::Specification.new do |spec|
  spec.name = "fb"
  spec.version = Fb::VERSION

  spec.authors = ["Brent Rowland", "Popa Adrian Marius", "Mikhail Mikhaliov", "REDSOFT", "Rolando Daniel Arnaudo"]
  spec.email = ["rowland@rowlandresearch.com", "mapopa@gmail.com", "legiar@gmail.com", "support@red-soft.ru",
                "rolando.arnaudo@gmail.com"]

  spec.summary = "Firebird database driver"
  spec.description = "Ruby Firebird Extension Library"
  spec.homepage = "http://github.com/rollyar/fb"

  spec.requirements = "Firebird client library fbclient.dll, libfbclient.so or Firebird.framework."

  spec.license = "MIT"
  spec.required_ruby_version = ">= 2.6.0"

  spec.homepage = "https://github.com/rollyar/fb"
  spec.metadata["source_code_uri"] = "https://github.com/rollyar/fb"
  spec.metadata["changelog_uri"] = "https://github.com/rollyar/fb/blob/master/CHANGELOG.md"

  spec.files = `git ls-files README.md ext lib`.split
  spec.extensions = ["ext/fb/extconf.rb"]
  spec.require_paths = ["lib"]

  spec.add_development_dependency "minitest", "~> 5.0"
  spec.add_development_dependency "rake", "~> 13.0"
  spec.add_development_dependency "rake-compiler", "~> 1.2"
end
