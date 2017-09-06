package Catmandu::Store::File::MediaHaven::Bag;

our $VERSION = '0.02';

use Catmandu::Sane;
use Moo;
use Carp;
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
            $mh->export($id, sub {
                my $data = shift;
                # Support the Dancer send_file "write" callback
                if ($out->can('syswrite')) {
                    $bytes += $out->syswrite($data);
                }
                else {
                    $bytes += $out->write($data);
                }
            });

            $out->close();

            $bytes;
        }
    );
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

    return $self->_get($result,$id);
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
