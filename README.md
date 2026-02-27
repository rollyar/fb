# fb - Ruby Firebird Extension Library

A Ruby extension for connecting to Firebird databases.

## Requirements

- Ruby 2.0+ (tested with 3.3)
- Firebird 2.0+ (for RETURNING support)
- Firebird 3.0+ or 4.0+ for full type support
- Firebird client libraries

## Installation

```bash
gem build fb.gemspec
gem install fb-*.gem
```

Or add to your Gemfile:

```ruby
gem 'fb'
```

## Connection

```ruby
require 'fb'

db = Fb::Database.new(
  database: 'localhost:/path/to/database.fdb',
  username: 'sysdba',
  password: 'masterkey',
  charset: 'NONE'
)

# Connect to database
conn = db.connect

# Or create if doesn't exist
conn = db.create.connect
```

### Connection Parameters

| Parameter | Description | Default |
|-----------|-------------|---------|
| `:database` | Database path or connection string | (required) |
| `:username` | Database username | `sysdba` |
| `:password` | Database password | `masterkey` |
| `:charset` | Character set for connection | `NONE` |
| `:role` | Database role | `nil` |
| `:encoding` | Ruby encoding for strings | `ASCII-8BIT` |
| `:page_size` | Database page size | `4096` |
| `:downcase_names` | Return column names in lowercase | `nil` |

### Encoding

By default, `@encoding` is set to `ASCII-8BIT` (binary-safe). This is the recommended setting when using `charset: 'NONE'` in Firebird.

If your database uses UTF-8 charset, you can set:

```ruby
db = Fb::Database.new(
  database: 'localhost:/path/to/database.fdb',
  username: 'sysdba',
  password: 'masterkey',
  charset: 'UTF8',
  encoding: 'UTF-8'
)
```

**Note:** BLOB data type applies the connection encoding. Text columns (VARCHAR, CHAR) return strings without encoding conversion.

## Querying

### Simple queries

```ruby
# Execute INSERT/UPDATE/DELETE (returns rows affected)
rows = conn.execute("INSERT INTO users (name) VALUES (?)", "John")
# => 1

# Query with parameters
conn.query("SELECT * FROM users WHERE id > ?", 10).each do |row|
  puts row[0]
end
```

### Fetch modes

```ruby
# Array access (by index)
conn.query("SELECT id, name FROM users").each do |row|
  puts "ID: #{row[0]}, Name: #{row[1]}"
end

# Hash access (by column name)
conn.query(:hash, "SELECT id, name FROM users").each do |row|
  puts "ID: #{row['ID']}, Name: #{row['NAME']}"
end
```

### Using cursors

```ruby
cursor = conn.execute("SELECT * FROM users")
while row = cursor.fetch(:hash)
  puts row['name']
end
cursor.close

# Or with block (auto-closes)
conn.execute("SELECT * FROM users") do |cursor|
  cursor.each do |row|
    puts row['name']
  end
end
```

## Transactions

### Auto-commit mode

```ruby
conn.transaction do
  conn.execute("INSERT INTO users (name) VALUES (?)", "John")
  conn.execute("INSERT INTO users (name) VALUES (?)", "Jane")
end
# Automatically commits if no exception
```

### Manual transactions

```ruby
conn.transaction

conn.execute("INSERT INTO users (name) VALUES (?)", "John")
conn.execute("INSERT INTO users (name) VALUES (?)", "Jane")

conn.commit  # or conn.rollback
```

### Transaction options

```ruby
# Snapshot (READ COMMITTED is default)
conn.transaction("SNAPSHOT") do
  # consistent view
end

# Read committed
conn.transaction("READ COMMITTED") do
  # sees committed changes
end
```

## RETURNING Clause

The `execute()` method supports `INSERT`, `UPDATE`, and `DELETE` with `RETURNING` clause:
- **INSERT RETURNING**: Firebird 2.0+
- **UPDATE RETURNING**: Firebird 2.1+
- **DELETE RETURNING**: Firebird 2.5+

Returns a `Hash`:
- `:returning` - Array of returned values
- `:rows_affected` - Number of rows affected

### Examples

```ruby
# INSERT with RETURNING (auto-generated ID)
result = conn.execute(
  "INSERT INTO users (name, email) VALUES (?, ?) RETURNING id",
  "John Doe", "john@example.com"
)
result[:returning]     # => [1]
result[:rows_affected] # => 1

# UPDATE with RETURNING
result = conn.execute(
  "UPDATE users SET name = ? WHERE email = ? RETURNING id, name",
  "Jane Doe", "john@example.com"
)
result[:returning]     # => [1, "John Doe"]
result[:rows_affected] # => 1

# DELETE with RETURNING
result = conn.execute(
  "DELETE FROM users WHERE id = ? RETURNING id, name",
  1
)
result[:returning]     # => [1, "Jane Doe"]
result[:rows_affected] # => 1

# When no rows match
result = conn.execute(
  "UPDATE users SET name = ? WHERE id = ? RETURNING id",
  "NewName", 9999  # Non-existent ID
)
result[:returning]     # => []
result[:rows_affected] # => 0

# RETURNING with NULL values
result = conn.execute(
  "INSERT INTO users (name) VALUES (?) RETURNING email",
  "John"
)
result[:returning]     # => [nil]
result[:rows_affected] # => 1
```

## Data Types

The following data types are supported:

| Firebird Type | Ruby Type |
|--------------|-----------|
| `CHAR`, `VARCHAR` | String |
| `BLOB` | String |
| `SMALLINT` | Integer |
| `INTEGER` | Integer |
| `BIGINT` | Integer |
| `INT128` | Integer/BigDecimal (Firebird 4+) |
| `NUMERIC(n)` | Integer/BigDecimal |
| `DECIMAL(n)` | Integer/BigDecimal |
| `DECFLOAT(16)`, `DECFLOAT(34)` | BigDecimal (Firebird 4+) |
| `FLOAT` | Float |
| `DOUBLE PRECISION` | Float |
| `DATE` | Date |
| `TIME` | Time |
| `TIMESTAMP` | DateTime |
| `TIMESTAMP WITH TIME ZONE` | DateTime (Firebird 4+) |
| `TIME WITH TIME ZONE` | Time (Firebird 4+) |
| `BOOLEAN` | true/false (Firebird 3+) |

## Running Tests

```bash
# Install dependencies
bundle install

# Run tests
ruby -Ilib:test test/FbTestSuite.rb

# Or use rake
rake test
```

### Test Environment Variables

```bash
FIREBIRD_HOST=localhost
FIREBIRD_PORT=3050
FIREBIRD_USER=sysdba
FIREBIRD_PASSWORD=masterkey
FIREBIRD_DATA_DIR=/path/to/data
```

## License

MIT License

Note: UUID is supported via GEN_UUID() function - stores as CHAR(16) OCTETS or VARCHAR(36)

