Gem::Specification.new do |spec|
  spec.name          = "quill-ruby"
  spec.version       = "0.1.0"
  spec.summary       = "Quill"
  spec.description   = "Quill sdk for Ruby. See quill.co for more information."
  spec.authors       = ["Shawn Magee, Albert Yan"]
  spec.email         = ["shawn@quill.co"]
  spec.files         = Dir["lib/**/*", "examples/**/*", "LICENSE", "README.md"]
  spec.homepage      = "https://github.com/quill-sql/quill-ruby"
  spec.license       = "MIT"

  # Core dependencies
  spec.add_runtime_dependency "json", ">= 2.0"
  spec.add_runtime_dependency "activesupport", "~> 7.0.0"
  spec.add_runtime_dependency "click_house", "~> 2.1.2"
  spec.add_runtime_dependency "redis"
  spec.add_runtime_dependency "dotenv"

  # Development dependencies
  spec.add_development_dependency "rubocop"
  spec.add_development_dependency "sinatra"
  spec.add_development_dependency "sinatra-cors"

  spec.required_ruby_version = ">= 2.7.0"
end