package Catmandu::Store::File::MediaHaven::Index;

our $VERSION = '0.02';

use Catmandu::Sane;
use Moo;
use Carp;
use POSIX qw(ceil);
use namespace::clean;

use Data::Dumper;

with 'Catmandu::Bag', 'Catmandu::FileBag::Index';

sub generator {
    my ($self) = @_;

    my $mh  = $self->store->mh;

    my $res = $mh->search();

    sub {
        state $results = $res->{mediaDataList};
        state $total   = $res->{totalNrOfResults};
        state $index   = 0;

        $index++;

        if (@$results > 1) {
            my $hit =  shift @$results;
            return $self->hit2rec($hit);
        }
        elsif ($index < $total) {
            my $res = $mh->search(undef, start => $index+1);

            $results = $res->{mediaDataList};
            $index++;

            my $hit = shift @$results;

            return $self->hit2rec($hit);
        }
        return undef;
    };
}

sub hit2rec {
    my ($self,$hit) = @_;

    if ($self->store->id_fixer) {
        return $self->store->id_fixer->fix($hit);
    }
    else {
        my $id = $hit->{externalId};
        return +{_id => $id};
    }
}

sub exists {
    my ($self, $id) = @_;

    croak "Need a key" unless defined $id;

    my $res = $self->store->mh->record($id);

    defined($res);
}

sub add {
    my ($self, $data) = @_;

    croak "Add is not supported in the MediaHaven FileStore";
}

sub get {
    my ($self, $id) = @_;

    my $res = $self->store->mh->record($id);

    if ($res) {
        return +{_id => $id};
    }
    else {
        return undef;
    }
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

Catmandu::Store::File::MediaHaven::Index - Index of all "Folders" in a MediaHaven database

=head1 SYNOPSIS

    use Catmandu;

    my $store = Catmandu->store('File::MediaHaven'
                        , url      => '...'
                        , username => '...'
                        , password => '...' );

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

Execute C<callback> on every "folder" in the MediaHaven store. See L<Catmandu::Iterable> for more
iterator functions

=head2 exists($id)

Returns true when a "folder" with identifier $id exists.

=head2 add($hash)

Not implemeted

=head2 get($id)

Returns a hash containing the metadata of the folder. In the MediaHaven store this hash
will contain only the "folder" idenitifier.

=head2 files($id)

Return the L<Catmandu::Store::File::MediaHaven::Bag> that contains all "files" in the "folder"
with identifier $id.

=head2 delete($id)

Not implemeted

=head2 delete_all()

Not implemeted

=head1 SEE ALSO

L<Catmandu::Store::File::MediaHaven::Bag> ,
L<Catmandu::Store::File::MediaHaven> ,
L<Catmandu::FileBag::Index> ,
L<Catmandu::Plugin::SideCar> ,
L<Catmandu::Bag> ,
L<Catmandu::Iterable>

=cut
