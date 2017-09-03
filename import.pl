use strict;
use warnings;

use DBI;

#GLOBALS
my $db_dsn = "DBI:mysql:database=c9;host=localhost;port=3306";
my $db_user = "mriedmann";
my $db_pass = "";

my $filename_prefix = "files/";

my %filenames = (
    "categories" => { 
        filename => "eggnog4.functional_categories.txt",
        tablename => "eggnog_functional_categories",
        tablecreate => "meta_category VARCHAR(255) NOT NULL, functional_category CHAR(1) NOT NULL PRIMARY KEY, description VARCHAR(255) NOT NULL",
        tableinsert => "(meta_category, functional_category, description) values (?, ?, ?)",
        importfunction => \&import_categories
    },
    "species" => {
        filename => "eggnog4.species_list.txt",
        tablename => "eggnog_species",
        tablecreate => "species_name VARCHAR(255) NOT NULL, taxon_id INT(10) NOT NULL PRIMARY KEY",
        tableinsert => "(species_name, taxon_id) values (?, ?)",
        importfunction => \&import_species
    },
    "annotations" => {
        filename => "meNOG.annotations.tsv",
        tablename => "meNOG_annotations",
        tablecreate => "group_name VARCHAR(255) NOT NULL PRIMARY KEY, protein_count INT(8) NOT NULL, species_count INT(8) NOT NULL, functional_category CHAR(1) NOT NULL, description VARCHAR(255) NOT NULL",
        tableinsert => "(group_name, protein_count, species_count, functional_category, description ) values (?, ?, ?, ?, ?)",
        importfunction => \&import_annotations
    },
    "members" => {
        filename => "meNOG.members.tsv",
        tablename => "meNOG_members",
        tablecreate => "group_name VARCHAR(255) NOT NULL PRIMARY KEY, protein_count INT(8) NOT NULL, species_count INT(8) NOT NULL, functional_category CHAR(1) NOT NULL",
        tableinsert => "(group_name, protein_count, species_count, functional_category ) values (?, ?, ?, ?)",
        importfunction => \&import_members
    },
    "groups" => {
        tablename => "meNOG_groups",
        tablecreate => "group_name VARCHAR(255) NOT NULL, taxon_id INT(10) NOT NULL, protein_id VARCHAR(32) NOT NULL, PRIMARY KEY(group_name, taxon_id, protein_id)",
        tableinsert => "(group_name, taxon_id, protein_id) values (?, ?, ?)",
    }
);

#INIT
my $dbh = DBI->connect($db_dsn, $db_user, $db_pass);

#SUBS
sub db_exec {
    my @sqls = @_;
    foreach my $sql (@sqls) {
        $dbh->do($sql)
            or die "do statement failed: $dbh->errstr()";
    }
}

sub insert_data {
    my ($name,  @data) = @_;
    
    print join("\t", @data) . "\n";
    
    my $tablename = $filenames{$name}{'tablename'};
    my $tableinsertpart = $filenames{$name}{'tableinsert'};
    my $sql = "INSERT INTO `$tablename` " . $tableinsertpart . ";";

    # prepare your statement for connecting to the database
    my $stm = $dbh->prepare($sql)
        or die "prepare statement failed: $dbh->errstr()";;
    
    # execute your SQL statement
    $stm->execute(@data) or die "execute failed: $dbh->errstr()";;
}

sub create_table {
    my ($name) = @_;
    my $tablename = $filenames{$name}{'tablename'};
    my $tablecolumns = $filenames{$name}{'tablecreate'};
    db_exec(
        "DROP TABLE IF EXISTS `$tablename`;",
        "CREATE TABLE `$tablename` ( $tablecolumns ) ENGINE=InnoDB;" 
    );
}

sub import_file {
    my ($name) = @_;
    
    my $filename = $filenames{$name}{'filename'};
    open(my $fh, "<", $filename_prefix . $filename)
    	or die "Can't open < input.txt: $!";
    	
    my $importfunction = $filenames{$name}{'importfunction'};;
    my $context = {};
    
    while(my $line = <$fh>){
        chomp $line;
        $importfunction->($line, $context);
    }
    
    close($fh);
}

sub import_categories {
    my ($line, $context) = @_;
    
    if(not ($line =~ /^ /)){
        $context->{'main_cat'} = $line;
    } else {
        my @matches = $line =~ /^ \[([A-Z])\] (.*)$/;
        if(scalar(@matches) gt 0){
            my $tag = $matches[0];
            my $name = $matches[1];
            my @data = ($context->{'main_cat'}, $tag, $name);
            
            insert_data("categories", @data);
        }
    }
}

sub import_species {
    my ($line, $context) = @_;
    
    my @data = parse_tsv_line($line, 0, 2);
    insert_data("species", @data);
}

sub import_annotations {
    my ($line, $context) = @_;
    
    my @data = parse_tsv_line($line, 1, 5);
    insert_data("annotations", @data);
}

sub import_members {
    my ($line, $context) = @_;
    
    my @data = parse_tsv_line($line, 1, 5);
    print join(',', @data) . "\n";
    my $group_name = $data[0];
    my @taxprots = split ',',$data[4];
    print join(',', @taxprots) . "\n";
    foreach my $taxprot (@taxprots) {
        print $taxprot . "\n";
        my ($taxon_id, $protein_id) = $taxprot =~ /([0-9]+)\.(.*)/;
        insert_data("groups", ($group_name, $taxon_id, $protein_id));
    }
    
    insert_data("members", splice(@data, 0, 4));
}

sub parse_tsv_line {
    my ($line, $startcol, $colcount) = @_;
    
    my @fields = split "\t", $line;
    if(scalar(@fields) ge $colcount + $startcol) {
        return splice @fields, $startcol, $colcount;
    }
    return [];
}

#MAIN

#create_table("categories");
#import_file("categories");

#create_table("species");
#import_file("species");

#create_table("annotations");
#import_file("annotations");

create_table("groups");
create_table("members");
import_file("members");

$dbh->disconnect();