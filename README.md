# Fb - Ruby Firebird Extension Library

This is a Ruby driver for the [Firebird](https://firebirdsql.org/)  database.

TODO: Delete this and the text below, and describe your gem

Welcome to FB! In this directory, you'll find the files you need to be able to package up your Ruby library into a gem. Put your Ruby code in the file `lib/fb`. To experiment with that code, run `bin/console` for an interactive prompt.

## Installation

Install the gem and add to the application's Gemfile by executing:

    $ bundle add fb

If bundler is not being used to manage dependencies, install the gem by executing:

    $ gem install fb

## Usage

require 'fb'

# The library contains on module, Fb.
# Within Fb are four primary classes, Fb::Database, Fb::Connection, Fb::Cursor and Fb::Error.
# For convenience, we'll include these classes into the current context.

include Fb

# The Database class acts as a factory for Connections.
# It can also create and drop databases.

db = Database.new(
  :database => "localhost:c:/var/fbdata/readme.fdb",
  :username => 'sysdba',
  :password => 'masterkey')

# :database is the only parameter without a default.

# Let's connect to the database, creating it if it doesn't already exist.

conn = db.connect rescue db.create.connect

# We'll need to create the database schema if this is a new database.

conn.execute("CREATE TABLE TEST (ID INT NOT NULL PRIMARY KEY, NAME VARCHAR(20))") if !conn.table_names.include?("TEST")

# Let's insert some test data using a parameterized query.  Note the use of question marks for place holders.

10.times {|id| conn.execute("INSERT INTO TEST VALUES (?, ?)", id, "John #{id}") }

# Here we'll conduct a spot check of the data we have just inserted.

ary = conn.query("SELECT * FROM TEST WHERE ID = 0 OR ID = 9")
ary.each {|row| puts "ID: #{row[0]}, Name: #{row[1]}" }

# Don't like tying yourself down to column offsets?

ary = conn.query(:hash, "SELECT * FROM TEST WHERE ID = 0 OR ID = 9")
ary.each {|row| puts "ID: #{row['ID']}, Name: #{row['NAME']}" }

# Let's change all the names.

total_updated = 0
conn.execute("SELECT ID FROM TEST") do |cursor|
  cursor.each do |row|
    updated = conn.execute("UPDATE TEST SET NAME = ? WHERE ID = ?", "John Doe #{row[0]}", row[0])
    total_updated += updated
  end
end
puts "We updated a total of #{total_updated} rows."

# Actually, I only need the first and last rows.

deleted = conn.execute("DELETE FROM TEST WHERE ID > ? AND ID < ?", 0, 9)
puts "Expecting to delete 8 rows, we have deleted #{deleted}."

# Using a simple, per-connection transaction strategy, we'll demonstrate rollback and commit.

conn.transaction

for i in 10..1000
  conn.execute("INSERT INTO TEST VALUES (?, ?)", i, "Jane #{i}")
end

# What was I thinking?  Let's roll that back.

conn.rollback

# Are they still there?

janes = conn.query("SELECT * FROM TEST WHERE ID >= 10")
puts "Expecting zero rows, we find #{janes.size} Janes."

# Let's try again.

conn.transaction

10.upto(19) do |i|
  conn.execute("INSERT INTO TEST (ID, NAME) VALUES (?, ?)", i, "Sue #{i}")
end

# That's more like it.

conn.commit

# It's important to close your cursor when you're done with it.

cursor = conn.execute("SELECT * FROM TEST")
while row = cursor.fetch(:hash)
  break if row['NAME'] =~ /e 13/
end
cursor.close

# You may find it easier to use a block.

conn.execute("SELECT * FROM TEST") do |cursor|
  while row = cursor.fetch(:hash)
    break if row['NAME'] =~ /e 13/
  end
end

# That way the cursor always gets closed, even if an exception is raised.
# Transactions work the same way.  Here's one that should work.

conn.transaction do
  20.upto(25) do |i|
    conn.execute("INSERT INTO TEST VALUES (?, ?)", i, "George #{i}")
  end
end

# The transaction is automatically committed if no exception is raised in the block.
# We expect trouble in this next example, on account of our primary key.

begin
  conn.transaction do
    execute("INSERT INTO TEST VALUES (0, 'Trouble')")
    puts "This line should never be executed."
  end
rescue
  puts "Rescued."
end

# Is it there?

trouble = conn.query("SELECT * FROM TEST WHERE NAME = 'Trouble'")
puts "Expecting zero rows, we find #{trouble.size} 'Trouble' rows."

# How about demonstrating a more advanced transaction?
# First, we'll start a snapshot transaction.
# This should give us a consistent view of the database.

conn.transaction("SNAPSHOT") do

# Then, we'll open another connection to the database and insert some rows.

  Database.connect(:database => "localhost:c:/var/fbdata/readme.fdb") do |conn2|
    for i in 100...110
      conn2.execute("INSERT INTO TEST VALUES (?, ?)", i, "Hi #{i}")
    end
  end

# Now, let's see if we see them.

  hi = conn.query("SELECT * FROM TEST WHERE ID >= ?", 100)
  puts "Expecting zero rows, we find #{hi.size} Hi rows."
end

# Now we will try our example again, only with a READ COMMITTED transaction.

conn.transaction("READ COMMITTED") do

# Then, we'll open another connection to the database and insert some rows.

  Database.connect(:database => "localhost:c:/var/fbdata/readme.fdb") do |conn2|
    for i in 200...210
      conn2.execute("INSERT INTO TEST VALUES (?, ?)", i, "Hello #{i}")
    end
  end

# Now, let's see if we see them.

  hello = conn.query("SELECT * FROM TEST WHERE ID >= ?", 200)
  puts "Expecting ten rows, we find #{hello.size}."
end

# Don't forget to close up shop.

conn.close

# We could have called conn.drop.
# We could still call db.drop


## Development

After checking out the repo, run `bin/setup` to install dependencies. Then, run `rake test` to run the tests. You can also run `bin/console` for an interactive prompt that will allow you to experiment.

To install this gem onto your local machine, run `bundle exec rake install`. To release a new version, update the version number in `version.rb`, and then run `bundle exec rake release`, which will create a git tag for the version, push git commits and the created tag, and push the `.gem` file to [rubygems.org](https://rubygems.org).

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/rollyar/fb. This project is intended to be a safe, welcoming space for collaboration, and contributors are expected to adhere to the [code of conduct](https://github.com/rollyar/fb/blob/master/CODE_OF_CONDUCT.md).

## License

The gem is available as open source under the terms of the [MIT License](https://opensource.org/licenses/MIT).

## Code of Conduct

Everyone interacting in the Fb project's codebases, issue trackers, chat rooms and mailing lists is expected to follow the [code of conduct](https://github.com/rollyar/fb/blob/master/CODE_OF_CONDUCT.md).
