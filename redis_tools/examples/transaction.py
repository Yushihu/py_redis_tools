from redis import Redis

from ..transaction import Transaction


def incr(trans: Transaction, k):
    trans.watch(k)

    @trans.reading
    def r():
        v = trans.pipe.get(k)
        v = 0 if v is None else int(v)

        @trans.writing
        def w():
            trans.pipe.set(k, v + 1)


def decr(trans: Transaction, k):
    trans.watch(k)

    @trans.reading
    def r():
        v = trans.pipe.get(k)
        v = 0 if v is None else int(v)

        @trans.writing
        def w():
            trans.pipe.set(k, v - 1)


def incr_foo_decr_bar_trans(client: Redis):
    trans = Transaction(client)
    incr(trans, 'foo')
    decr(trans, 'bar')
    trans.execute()


def incr_foo_incr_bar_trans(client: Redis):
    trans = Transaction(client)
    incr(trans, 'foo')
    incr(trans, 'bar')
    trans.execute()


# legacy
# hard to break down the method into incr and decr to make them more reusable.
def incr_foo_decr_bar(client: Redis):
    def f(pipe):
        foo_v = pipe.get('foo')
        foo_v = 0 if foo_v is None else int(foo_v)
        bar_v = pipe.get('bar')
        bar_v = 0 if bar_v is None else int(bar_v)
        pipe.multi()
        pipe.set('foo', foo_v + 1)
        pipe.set('bar', bar_v - 1)

    client.transaction(f, 'foo', 'bar')


def incr_foo_incr_bar(client: Redis):
    def f(pipe):
        foo_v = pipe.get('foo')
        foo_v = 0 if foo_v is None else int(foo_v)
        bar_v = pipe.get('bar')
        bar_v = 0 if bar_v is None else int(bar_v)
        pipe.multi()
        pipe.set('foo', foo_v + 1)
        pipe.set('bar', bar_v + 1)

    client.transaction(f, 'foo', 'bar')


if __name__ == '__main__':
    c = Redis()
    incr_foo_incr_bar_trans(c)
    print(f'foo {c.get("foo")} bar {c.get("bar")}')
