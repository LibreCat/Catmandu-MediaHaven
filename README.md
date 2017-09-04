# NAME

Catmandu::MediaHaven - Tools to communicate with a MediaHaven server

# SYNOPSIS

    use Catmandu::MediaHaven;

    my $mh = Catmandu::MediaHaven->new(
                    url      => '...' ,
                    username => '...' ,
                    password => '...');

    my $result = $mh->search('nature', start => 0 , num => 100);

    die "search failed" unless defined($result);

    for my $res (@{$result->mediaDataList}) {
        my $id = $res->{externalId};
        my $date = $res->{data};

        print "$id $date\n";
    }

    my $record = $mh->record('q2136s817');
    my $date   = $record->{date};

    print "q2136s817 $date\n";

    $mh->export($id, sub {
       my $data = shift;
       print $data;
    });

# DESCRIPTION

The [Catmandu::MediaHaven](https://metacpan.org/pod/Catmandu::MediaHaven) module is a low end interface to the MediaHaven
REST api. See also: https://archief.viaa.be/mediahaven-rest-api

# METHODS

## new(url => ... , username => ... , password => ...)

Create a new connection to the MediaHaven server.

## search($query, start => ... , num => ...)

Execute a search query against the MediaHaven server and return the result\_list
as a HASH

## record($id)

Retrieve one record from the MediaHaven server based on an identifier. Returns
a HASH of results.

## export($id, $callback)

Export the binary content of a record from the MediaHaven server. The callback
will retrieve a stream of data when the download is available,

# SEE ALSO

[Catmandu::Importer::MediaHaven](https://metacpan.org/pod/Catmandu::Importer::MediaHaven)

# AUTHOR

- Patrick Hochstenbach, `<patrick.hochstenbach at ugent.be>`

# LICENSE AND COPYRIGHT

This program is free software; you can redistribute it and/or modify it under the terms
of either: the GNU General Public License as published by the Free Software Foundation;
or the Artistic License.

See [http://dev.perl.org/licenses/](http://dev.perl.org/licenses/) for more information.
