package acceptance_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"fairq"
	"fairq/client"
	"fairq/internal/rdb"
	"github.com/redis/go-redis/v9"
)

func TestACLAndTLS(t *testing.T) {
	n := newNode(t)
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	cert := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "fairq-local-acceptance"}, NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, IsCA: true, BasicConstraintsValid: true, DNSNames: []string{"localhost"}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}}
	der, err := x509.CreateCertificate(rand.Reader, cert, cert, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyBytes, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	certPath, keyPath := filepath.Join(n.dir, "server.crt"), filepath.Join(n.dir, "server.key")
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	if err = os.WriteFile(certPath, certPEM, 0600); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyBytes}), 0600); err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()
	n.args = append(n.args, "--tls-port", strconv.Itoa(port), "--tls-cert-file", certPath, "--tls-key-file", keyPath, "--tls-ca-cert-file", certPath, "--tls-auth-clients", "no")
	n.start()
	ctx := context.Background()
	o := n.options()
	admin := redis.NewClient(o.Simple())
	defer admin.Close()
	permissions := []any{"ACL", "SETUSER", "fairq-test", "on", ">isolated-fixture-password", "~fairq:v1:*", "-@all", "+info", "+time", "+scan", "+sscan", "+zscan", "+type", "+exists", "+del", "+hset", "+hget", "+hmget", "+hgetall", "+hdel", "+lpush", "+rpush", "+lpop", "+rpop", "+llen", "+lindex", "+lrange", "+sadd", "+srem", "+sismember", "+scard", "+smembers", "+zadd", "+zrem", "+zscore", "+zcard", "+zrange", "+eval", "+evalsha", "+script|load", "+ping", "+hello", "+select", "+client|setinfo"}
	if err = admin.Do(ctx, permissions...).Err(); err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(certPEM)
	secured := redis.UniversalOptions{Addrs: []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(port))}, Username: "fairq-test", Password: "isolated-fixture-password", TLSConfig: &tls.Config{RootCAs: roots, ServerName: "localhost", MinVersion: tls.VersionTLS12}, MaxRetries: -1}
	p, err := client.New(ctx, client.Config{Redis: secured})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if _, err = p.Enqueue(ctx, fairq.Task{ID: "secured", Type: "test"}, fairq.EnqueueOptions{GroupKey: "g"}); err != nil {
		t.Fatal(err)
	}
	s, err := rdb.New(ctx, secured)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	a, err := s.Dequeue(ctx, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.Heartbeat(ctx, []rdb.Attempt{a}, time.Minute); err != nil {
		t.Fatal(err)
	}
	if err = s.Done(ctx, a); err != nil {
		t.Fatal(err)
	}
	healthy(t, secured, 0)
	if err = s.Redis.Set(ctx, "outside-namespace", "forbidden", 0).Err(); err == nil || !strings.Contains(err.Error(), "NOPERM") {
		t.Fatal("ACL allowed foreign key", err)
	}
	bad := secured
	bad.TLSConfig = secured.TLSConfig.Clone()
	bad.TLSConfig.ServerName = "wrong.example"
	if c, e := client.New(ctx, client.Config{Redis: bad}); e == nil {
		c.Close()
		t.Fatal("TLS accepted wrong hostname")
	}
	t.Log("TLS trusted CA/hostname and namespace ACL passed; invalid hostname and foreign key rejected")
}

func TestNoEvictionOOMAndClusterRejection(t *testing.T) {
	t.Run("OOM", func(t *testing.T) {
		n := newNode(t)
		n.start()
		ctx := context.Background()
		seedTasks(t, n.options(), 1)
		s, err := rdb.New(ctx, n.options())
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		memory, err := s.Redis.Info(ctx, "memory").Result()
		if err != nil {
			t.Fatal(err)
		}
		var used int64
		for _, line := range strings.Split(memory, "\n") {
			if strings.HasPrefix(line, "used_memory:") {
				used, _ = strconv.ParseInt(strings.TrimSpace(strings.TrimPrefix(line, "used_memory:")), 10, 64)
			}
		}
		if used == 0 {
			t.Fatal("no memory reading")
		}
		if err = s.Redis.ConfigSet(ctx, "maxmemory", strconv.FormatInt(used/2, 10)).Err(); err != nil {
			t.Fatal(err)
		}
		p, err := client.New(ctx, client.Config{Redis: n.options()})
		if err != nil {
			t.Fatal(err)
		}
		defer p.Close()
		info, err := p.Enqueue(ctx, fairq.Task{ID: "oom", Type: "test"}, fairq.EnqueueOptions{GroupKey: "g"})
		if !fairq.IsCode(err, fairq.Internal) || info.ID != "oom" {
			t.Fatal(info, err)
		}
		if !strings.Contains(err.(*fairq.Error).Err.Error(), "OOM") {
			t.Fatal("expected actual Redis OOM", err)
		}
		if s.Redis.Exists(ctx, rdb.TaskKey("0")).Val() != 1 {
			t.Fatal("evicted existing task")
		}
		if err = s.Redis.ConfigSet(ctx, "maxmemory", "536870912").Err(); err != nil {
			t.Fatal(err)
		}
		healthy(t, n.options(), 1)
		t.Log("noeviction OOM returned INTERNAL, preserved existing Hash, no partial enqueue; capacity restored and Inspector healthy")
	})
	t.Run("Cluster", func(t *testing.T) {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		busPort := ln.Addr().(*net.TCPAddr).Port
		ln.Close()
		n := newNode(t, "--cluster-enabled", "yes", "--cluster-config-file", "nodes.conf", "--cluster-port", strconv.Itoa(busPort))
		n.start()
		p, err := client.New(context.Background(), client.Config{Redis: n.options()})
		if err == nil {
			p.Close()
			t.Fatal("actual cluster accepted")
		}
		if !strings.Contains(err.Error(), "Cluster") {
			t.Fatal(err)
		}
	})
}
