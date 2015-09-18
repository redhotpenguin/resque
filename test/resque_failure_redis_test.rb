require 'test_helper'
require 'resque/failure/redis'

context "Resque::Failure::Redis" do
  setup do
    @bad_string    = [39, 52, 127, 86, 93, 95, 39].map { |c| c.chr }.join
    exception      = StandardError.exception(@bad_string)
    worker         = Resque::Worker.new(:test)
    queue          = "queue"
    payload        = { "class" => Object, "args" => 3 }
    @redis_backend = Resque::Failure::Redis.new(exception, worker, queue, payload)
  end

  test 'cleans up bad strings before saving the failure, in order to prevent errors on the resque UI' do
    # test assumption: the bad string should not be able to round trip though JSON
    @redis_backend.save
    Resque::Failure::Redis.all # should not raise an error
  end
end

describe ".each" do

  context 'order ASC' do
    setup do
      Resque.redis.flushall
      exception      = StandardError.exception("error")
      worker         = Resque::Worker.new(:test)
      queue          = "queue"
      n              = 0
      5.times do
        Resque::Failure::Redis.new(exception, worker, queue, {'class' => Object, 'args' => "failure #{n}"}).save
        n += 1
      end
    end

    test "should iterate over the failed tasks with ids in order" do
      ids = []
      Resque::Failure::Redis.each(0, 20, nil, nil, 'asc') do |id, _|
        ids << id
      end
      assert_equal([0,1,2,3,4], ids)
    end
    test "shold work with a below-queue-size limit" do
      ids = []
      Resque::Failure::Redis.each(0, 3, nil, nil, 'asc') do |id, _|
        ids << id
      end
      assert_equal([0,1,2], ids)
    end
    test "shold work with a below-queue-size limit and offset" do
      ids = []
      Resque::Failure::Redis.each(1, 3, nil, nil, 'asc') do |id, _|
        ids << id
      end
      assert_equal([1,2,3], ids)
    end
    test "shold work with an above-queue-size limit and offset" do
      ids = []
      Resque::Failure::Redis.each(2, 20, nil, nil, 'asc') do |id, _|
        ids << id
      end
      assert_equal([2,3,4], ids)
    end
    test "should allow getting single failure" do
      ids = []
      items = []
      Resque::Failure::Redis.each(0, 1, nil, nil, 'asc') do |id, item|
        ids << id
        items << item
      end
      assert_equal([0], ids)
      assert_equal('failure 0', items.first['payload']['args'])
    end
    test "should allow getting single failure from the middle of the list" do
      ids = []
      items = []
      Resque::Failure::Redis.each(2, 1, nil, nil, 'asc') do |id, item|
        ids << id
        items << item
      end
      assert_equal([2], ids)
      assert_equal('failure 2', items.first['payload']['args'])
    end
  end

  context 'order desc' do
    setup do
      Resque.redis.flushall
      exception      = StandardError.exception("error")
      worker         = Resque::Worker.new(:test)
      queue          = "queue"
      n              = 0
      5.times do
        Resque::Failure::Redis.new(exception, worker, queue, {'class' => Object, 'args' => "failure #{n}"}).save
        n += 1
      end
    end
    test "should iterate over the failed tasks with ids in reverse order" do
      ids = []
      Resque::Failure::Redis.each(0, 20, nil, nil, 'desc') do |id, _|
        ids << id
      end
      assert_equal([4,3,2,1,0], ids)
    end
    test "should work with a below-queue-size limit" do
      ids = []
      Resque::Failure::Redis.each(0, 3, nil, nil, 'desc') do |id, _|
        ids << id
      end
      assert_equal([2,1,0], ids)
    end
    test "should work with a below-queue-size limit and offset" do
      ids = []
      Resque::Failure::Redis.each(2, 3, nil, nil, 'desc') do |id, _|
        ids << id
      end
      assert_equal([4,3,2], ids)
    end
    test "should work with an above-queue-size limit and offset" do
      ids = []
      Resque::Failure::Redis.each(2, 20, nil, nil, 'desc') do |id, _|
        ids << id
      end
      assert_equal([4,3,2], ids)
    end
    test "should allow getting single failure" do
      ids = []
      items = []
      Resque::Failure::Redis.each(4, 1, nil, nil, 'desc') do |id, item|
        ids << id
        items << item
      end
      assert_equal([4], ids)
      assert_equal('failure 4', items.first['payload']['args'])
    end
    test "should allow getting single failure from the middle of the list" do
      ids = []
      items = []
      Resque::Failure::Redis.each(2, 1, nil, nil, 'desc') do |id, item|
        ids << id
        items << item
      end
      assert_equal([2], ids)
      assert_equal('failure 2', items.first['payload']['args'])
    end
    test "should allow use of oversize lmit" do
      ids = []
      items = []
      Resque::Failure::Redis.each(3, 20, nil, nil, 'desc') do |id, item|
        ids << id
        items << item
      end
      assert_equal([4,3], ids)
      assert_equal('failure 4', items.first['payload']['args'])
      assert_equal('failure 3', items.last['payload']['args'])
    end
  end
end
