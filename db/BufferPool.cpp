#include <db/BufferPool.h>
#include <db/Database.h>
#include <set>

using namespace db;

namespace db {
    struct EvictManager {
        struct PageMeta {
            const PageId *pd;
            const Page *page;
            int refCount;
            int order;

            friend bool operator==(const PageMeta &pm1, const PageMeta &pm2) {
                return pm1.pd->operator==(*pm2.pd);
            }
            friend bool operator<(const PageMeta &pm1, const PageMeta &pm2) {

                if (pm1.page->isDirty() && !pm2.page->isDirty()) {
                    return true;
                }
                if (pm2.page->isDirty() && !pm1.page->isDirty()) {
                    return false;
                }
                if (pm1.refCount == pm2.refCount) {
                    return pm1.order < pm2.order;
                }
                return pm1.refCount < pm2.refCount;
            }
        };

        long curOrder;
        std::set<PageMeta> metas;
        EvictManager() : curOrder(0), metas() {

        }

        void discardPage(const Page *page) {
            auto it = std::find_if(metas.begin(), metas.end(), [&page](const PageMeta &meta)->bool {
                return meta.page->getId() == page->getId();
            });
            metas.erase(it);
        }

        void onPageRef(const Page *page) {
            const auto *pid = dynamic_cast<const PageId *>(&page->getId());
            auto it = std::find_if(metas.begin(), metas.end(), [&page](const PageMeta &meta)->bool {
                return meta.page->getId() == page->getId();
            });
            if (it == metas.end()) {
                metas.insert({pid, page, 1, (int)curOrder});
                curOrder++;
            } else {
                auto update = *it;
                update.page =  page;
                update.refCount += 1;
                metas.erase(it);
                metas.insert(update);
            }
        }

        Page *getVictim() {
            PageMeta meta = *(metas.begin());
            metas.erase(metas.begin());
            return const_cast<Page *>(meta.page);
        }

    };
}



void BufferPool::evictPage() {
    auto evictManager = static_cast<EvictManager *>(helper);
    auto page = evictManager->getVictim();
    flushPage(&page->getId());
    pages.erase(&page->getId());
}

void BufferPool::flushAllPages() {
    for (auto p : pages) {
        if (p.second->isDirty() != std::nullopt) {
            flushPage(p.first);
        }
    }
}

void BufferPool::discardPage(const PageId *pid) {
    auto evictManager = static_cast<EvictManager *>(helper);
    auto it = pages.find(pid);
    if (it != pages.end()) {
        evictManager->discardPage(it->second);
        pages.erase(it);
    }
}

void BufferPool::flushPage(const PageId *pid) {
    Page *toFlush = pages[pid];
    if (toFlush->isDirty()) {
        DbFile *dbFile = Database::getCatalog().getDatabaseFile(pid->getTableId());
        //Write it back into the database
        dbFile->writePage(toFlush);
        toFlush->markDirty(std::nullopt);
    }
}

void BufferPool::flushPages(const TransactionId &tid) {
    for (auto p : pages) {
        if (p.second->isDirty() == tid) {
            flushPage(p.first);
        }
    }
}

void BufferPool::insertTuple(const TransactionId &tid, int tableId, Tuple *t) {
    DbFile *dbFile = Database::getCatalog().getDatabaseFile(tableId);
    auto evictManager = static_cast<EvictManager *>(helper);
    auto dirtyPages = dbFile->insertTuple(tid, *t);
    for (auto p : dirtyPages) {
        p->markDirty(tid);
        if (pages.find(&p->getId()) == pages.end()) {
            while (pages.size() >= numPages) {
                evictPage();
            }
        }
        evictManager->onPageRef(p);
        pages[&p->getId()] = p;
    }
}

void BufferPool::deleteTuple(const TransactionId &tid, Tuple *t) {
    auto evictManager = static_cast<EvictManager *>(helper);
    DbFile *dbFile = Database::getCatalog().getDatabaseFile(t->getRecordId()->getPageId()->getTableId());
    auto dirtyPages = dbFile->deleteTuple(tid, *t);
    for (auto p : dirtyPages) {
        p->markDirty(tid);
        if (pages.find(&p->getId()) == pages.end()) {
            while (pages.size() >= numPages) {
                evictPage();
            }
        }
        evictManager->onPageRef(p);
        pages[&p->getId()] = p;
    }
}


void BufferPool::initPageReplacementPolicy() {
    helper = new EvictManager();
}

void BufferPool::destroyPageReplacementPolicy() {
    auto evictManager = static_cast<EvictManager *>(helper);
    delete(evictManager);
}


void BufferPool::onPageRef(Page *page) {
    auto evictManager = static_cast<EvictManager *>(helper);
    evictManager->onPageRef(page);
}