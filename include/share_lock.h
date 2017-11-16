#ifndef __WRITE_FIRST_RW_LOCK_H
#define __WRITE_FIRST_RW_LOCK_H

#include <mutex>
#include <condition_variable>

class WfirstRWLock
{
	public:
		WfirstRWLock() = default;
		~WfirstRWLock() = default;
	public:
		void lock_read()
		{
			std::unique_lock<std::mutex> ulk(counter_mutex);
			cond_r.wait(ulk, [=]()->bool {return write_cnt == 0; });
			++read_cnt;
			printf("read lock\n");
		}
		void lock_write()
		{
			std::unique_lock<std::mutex> ulk(counter_mutex);
			++write_cnt;
			cond_w.wait(ulk, [=]()->bool {return read_cnt == 0 && !inwriteflag; });
			inwriteflag = true;
			printf("write lock\n");
		}
		void release_read()
		{
			std::unique_lock<std::mutex> ulk(counter_mutex);
			if (--read_cnt == 0 && write_cnt > 0)
			{
				cond_w.notify_one();
			}
			printf("read unlock\n");
		}
		void release_write()
		{
			std::unique_lock<std::mutex> ulk(counter_mutex);
			if (--write_cnt == 0)
			{
				cond_r.notify_all();
			}
			else
			{
				cond_w.notify_one();
			}
			inwriteflag = false;
			printf("write unlock\n");
		}

	private:
		volatile size_t read_cnt{ 0 };
		volatile size_t write_cnt{ 0 };
		volatile bool inwriteflag{ false };
		std::mutex counter_mutex;
		std::condition_variable cond_w;
		std::condition_variable cond_r;
};

template <typename _RWLockable>
class unique_writeguard
{
	public:
		explicit unique_writeguard(_RWLockable &rw_lockable)
			: rw_lockable_(rw_lockable)
		{
			rw_lockable_.lock_write();
		}
		~unique_writeguard()
		{
			rw_lockable_.release_write();
		}
	private:
		unique_writeguard() = delete;
		unique_writeguard(const unique_writeguard&) = delete;
		unique_writeguard& operator=(const unique_writeguard&) = delete;
	private:
		_RWLockable &rw_lockable_;
};
template <typename _RWLockable>
class unique_readguard
{
	public:
		explicit unique_readguard(_RWLockable &rw_lockable)
			: rw_lockable_(rw_lockable)
		{
			rw_lockable_.lock_read();
		}
		~unique_readguard()
		{
			rw_lockable_.release_read();
		}
	private:
		unique_readguard() = delete;
		unique_readguard(const unique_readguard&) = delete;
		unique_readguard& operator=(const unique_readguard&) = delete;
	private:
		_RWLockable &rw_lockable_;
};

#endif
