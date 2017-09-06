package Catmandu::Store::File::MediaHaven::Bag;

our $VERSION = '0.02';

use Catmandu::Sane;
use Moo;
use Carp;
use Date::Parse;
use POSIX qw(ceil);
use namespace::clean;

with 'Catmandu::Bag', 'Catmandu::FileBag';

sub generator {
    my ($self) = @_;

    my $mh  = $self->store->mh;

    my $res = $mh->record($self->name);

    sub {
        state $done = 0;

        return undef if $done;

        $done = 1;

        return $self->_get($res,$res->{originalFileName});
    };
}

sub _get {
    my ($self,$result,$key) = @_;

    my $mh  = $self->store->mh;

    return undef unless $result;

    return undef unless $result->{originalFileName} eq $key;

    my $md5;

    for my $prop (@{$result->{mdProperties}}) {
        if ($prop->{attribute} eq 'md5_viaa') {
            $md5 = $prop->{value};
        }
    }

    return +{
        _id          => $key,
        size         => -1,
        md5          => $md5 ? $md5 : 'none',
        created      => str2time($result->{archiveDate}),
        modified     => str2time($result->{lastModifiedDate}),
        content_type => 'application/zip',
        _stream      => sub {
            my $out   = $_[0];
            my $bytes = 0;
            $mh->export($self->name, sub {
                my $data = shift;
                # Support the Dancer send_file "write" callback
                if ($out->can('syswrite')) {
                    $bytes += $out->syswrite($data) || die "failed to write : $!";
                }
                else {
                    $bytes += $out->write($data) || die "failed to write : $!";;
                }
            });

            $out->close();

            $bytes;
        }
    };
}

sub exists {
    my ($self, $id) = @_;

    my $mh  = $self->store->mh;

    my $res = $mh->record($self->name);

    $res->{originalFileName} eq $id;
}

sub get {
    my ($self, $id) = @_;

    my $mh  = $self->store->mh;

    my $res = $mh->record($self->name);

    return $self->_get($res,$id);
}

sub add {
    my ($self, $data) = @_;
    croak "Add is not supported in the MediaHaven FileStore";
}

sub delete {
    my ($self, $id) = @_;
    croak "Delete is not supported in the MediaHaven FileStore";
}

sub delete_all {
    my ($self) = @_;
    croak "Delete is not supported in the MediaHaven FileStore";
}

sub commit {
    return 1;
}

1;

__END__

=pod

=head1 NAME

Catmandu::Store::File::MediaHaven::Bag - Index of all "files" in a Catmandu::Store::File::MediaHaven "folder"

=head1 SYNOPSIS

    use Catmandu;

    my $store = Catmandu->store('File::MediaHaven' , root => 't/data');

    my $index = $store->index;

    # List all containers
    $index->each(sub {
        my $container = shift;

        print "%s\n" , $container->{_id};
    });

    # Get a folder
    my $folder = $index->get(1234);

    # Get the files in an folder
    my $files = $index->files(1234);

    $files->each(sub {
        my $file = shift;

        my $name         = $file->_id;
        my $size         = $file->size;
        my $content_type = $file->content_type;
        my $created      = $file->created;
        my $modified     = $file->modified;

        $file->stream(IO::File->new(">/tmp/$name"), file);
    });

    # Retrieve a file
    my $file = $files->get("data.dat");

    # Stream a file to an IO::Handle
    $files->stream(IO::File->new(">data.dat"),$file);

=head1 METHODS

=head2 each(\&callback)

Execute C<callback> on every "file" in the MediaHaven store "folder". See L<Catmandu::Iterable> for more
iterator functions

=head2 exists($name)

Returns true when a "file" with identifier $name exists.

=head2 add($hash)

Not implemeted

=head2 get($id)

Returns a hash containing the metadata of the file. The hash contains:

    * _id : the file name
    * size : file file size
    * content_type : the content_type
    * created : the creation date of the file
    * modified : the modification date of the file
    * _stream: a callback function to write the contents of a file to an L<IO::Handle>

If is very much advised to use the C<stream> method below to retrieve files from
the store.

=head2 delete($name)

Not implemeted

=head2 delete_all()

Not implemeted

=head2 upload(IO::Handle,$name)

Not implemeted

=head2 stream(IO::Handle,$file)

Write the contents of the $file returned by C<get> to the IO::Handle.

=head1 SEE ALSO

L<Catmandu::Store::File::MediaHaven::Bag> ,
L<Catmandu::Store::File::MediaHaven> ,
L<Catmandu::FileBag::Index> ,
L<Catmandu::Plugin::SideCar> ,
L<Catmandu::Bag> ,
L<Catmandu::Iterable>

=cut
