package Catmandu::MediaHaven;

use Moo;
use LWP::Simple;
use URI::Escape;
use JSON;
use LWP;
use Carp;
use Catmandu;
use Cache::LRU;
use REST::Client;

with 'Catmandu::Logger';

has 'url'        => (is => 'ro' , required => 1);
has 'username'   => (is => 'ro' , required => 1);
has 'password'   => (is => 'ro' , required => 1);
has 'sleep'      => (is => 'ro' , default => sub { 1 });

has 'cache'      => (is => 'lazy');
has 'cache_size' => (is => 'ro' , default => '1000');

sub _build_cache {
    my $self = shift;

    return Cache::LRU->new(size => $self->cache_size);
}

sub search {
    my ($self,$query,%opts) = @_;

    my @param = ();

    if (defined($query) && length($query)) {
        push @param , sprintf("q=%s",uri_escape($query));
    }

    if ($opts{start}) {
        push @param , sprintf("startIndex=%d",$opts{start});
    }

    if ($opts{num}) {
        push @param , sprintf("nrOfResults=%d",$opts{num});
    }

    $self->log->info("searching with params: " . join("&",@param));

    my $res = $self->_rest_get(@param);

    for my $hit (@{$res->{mediaDataList}}) {
        my $id;

        INNER: for my $prop (@{ $hit->{mdProperties} }) {
           if ($prop->{attribute} eq 'dc_identifier_localid') {
                $id = $prop->{value};
                        $id =~ s{^\S+:}{};
                last INNER;
           }
        }

        $self->cache->set($id => $hit) if defined($id);
    }

    $res;
}

sub record {
    my ($self,$id) = @_;

    if (my $hit = $self->cache->get($id)) {
        return $hit;
    }

    my $query;

    if (length $id > 30) {
        $query = "q=%2B(MediaObjectFragmentdcidentifierlocalid:%22archive.ugent.be%3A$id%22)";
    }
    else {
        $query = "q=%2B(MediaObjectExternalId:$id)";
    }

    my $res = $self->_rest_get($query);

    if (exists $res->{code} && $res->{code} eq 'ESERVER' && exists $res->{status}) {
        croak "error - query '$query' failed";
    }

    if ($res->{mediaDataList}) {
        return $res->{mediaDataList}->[0];
    }
    else {
        return undef;
    }
}

sub export {
    my ($self,$id,$callback) = @_;

    my $record = $self->record($id);

    return undef unless $record;

    my $mediaObjectId = $record->{mediaObjectId};

    return undef unless $mediaObjectId;

    my $media_url = sprintf "%s/%s/export" , $self->_rest_base , $mediaObjectId;

    my ($export_job,$next) = $self->_post_json($media_url);

    return undef unless $export_job;

    my $downloadUrl;

    while (1) {
        my $exportId = $export_job->[0]->{exportId};
        my $status   = $export_job->[0]->{status};

        $self->log->debug("exportId = $exportId ; status = $status");

        last if $status =~ /^(failed|cancelled)$/;

        $downloadUrl  = $export_job->[0]->{downloadUrl};

        if ($downloadUrl =~ /^htt/) {
            last;
        }

        $self->log->debug("sleep " . $self->sleep);
        sleep $self->sleep;

        $export_job = $self->_get_json($next);
    }

    my $rest_url = $self->_rest_base($downloadUrl);

    $self->log->debug("download: $rest_url");

    my $browser  = LWP::UserAgent->new();
    my $response = $browser->get($rest_url, ':content_cb' => $callback);
}

sub _get_json {
    my ($self,$url) = @_;

    $self->log->debug($url);

    my $client = REST::Client->new();
    $client->GET($url);
    my $json = $client->responseContent();

    decode_json $json;
}

sub _post_json {
    my ($self,$url) = @_;

    $self->log->debug($url);

    my $client = REST::Client->new();
    $client->POST($url);
    my $json = $client->responseContent();

    my $location = $self->_rest_base( $client->responseHeader('Location') );

    my $perl = decode_json $json;

    ($perl,$location);
}

sub _rest_base {
    my ($self,$url) = @_;

    my $authen    = sprintf "%s:%s" , uri_escape($self->username) , uri_escape($self->password);
    my $media_url = $url // $self->url;

    $media_url =~ s{https://}{};
    $media_url = 'https://' . $authen . '@' . $media_url;

    $media_url;
}

sub _rest_get {
    my ($self,@param) = @_;

    my $media_url = $self->_rest_base . '?';

    $media_url .= join("&",@param);

    $self->_get_json($media_url);
}

1;
