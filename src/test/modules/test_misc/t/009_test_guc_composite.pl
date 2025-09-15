# Copyright (c) 2024-2025, PostgreSQL Global Development Group

# Tests composite GUC parameters

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('main');
$node->init;
$node->start;

# Test ALTER SYSTEM command:
# ALTER SYSTEM must only write changed parameter in .auto.conf file
$node->safe_psql(
    'postgres',
    "ALTER SYSTEM SET replica->max_delay = 42"
);

# Check postgresql.auto.conf
my $auto_conf_path = $node->data_dir . '/postgresql.auto.conf';
my $expected_line = "replica = {enable_connections: \'false\', max_delay: \'42\', max_slot_size: \'0\'}";
my $found = 0;
open(my $fh, '<', $auto_conf_path) or die "Cannot open file: $!";
while (my $line = <$fh>) {
    chomp $line;
    if ($line =~ /^\s*\Q$expected_line\E\s*$/) {
        $found = 1;
        last;
    }
}
close($fh);

ok($found, 'Composite parameter setting found in postgresql.auto.conf');

#reload config
$node->safe_psql('postgres', 'SELECT pg_reload_conf()');

# Check that parameter was applied
my $current_value = $node->safe_psql(
    'postgres',
    "SHOW replica->max_delay"
);

is($current_value, '42', 'Composite parameter value is correctly set');

done_testing();
