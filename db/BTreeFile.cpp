#include <db/BTreeFile.h>

using namespace db;

BTreeLeafPage *BTreeFile::findLeafPage(TransactionId tid, PagesMap &dirtypages, BTreePageId *pid, Permissions perm,
                                       const Field *f) {
    // some code goes here
    //case 1. it is a leaf page, return directly
    if(pid->getType() == BTreePageType::LEAF){
        return (BTreeLeafPage*) getPage(tid,dirtypages,pid,perm);
    }
    auto *page = (BTreeInternalPage*) getPage(tid,dirtypages,pid,perm);
    auto it = page->begin();
    // if file is null find on its left child
    if(!f){
        if(it != page->end()){
            return findLeafPage(tid,dirtypages, (*it).getLeftChild(),perm,f);
        }
        return nullptr;
    }
    if (it != page->end()) {
        auto &next =  *it;
        // otherwise, find all its child's left children
        while(it != page->end()){
            next = *it;
            auto *key = next.getKey();
            if(f->compare(Op::LESS_THAN_OR_EQ,key)){
                return findLeafPage(tid,dirtypages,next.getLeftChild(),perm,f);
            }
            ++it;
        }
        return findLeafPage(tid,dirtypages,next.getRightChild(),perm,f);
    }

    return nullptr;


}

BTreeLeafPage *BTreeFile::splitLeafPage(TransactionId tid, PagesMap &dirtypages, BTreeLeafPage *page, const Field *field) {

    int half = page->getNumTuples()/2;
    auto newPage = (BTreeLeafPage *)getEmptyPage(tid,dirtypages,BTreePageType::LEAF);
    auto it = page->rbegin();
    while(it != page->rend() && half > 0){
        auto &tup = *it;
        //这里要先删除在插入，就很坑
        page->deleteTuple(&tup);
        newPage->insertTuple(&tup);
        --half;
    }
    //2. 获取当前要插入的父节点，并插入新节点
    auto &up = *it;
    auto parentPage = getParentWithEmptySlots(tid, dirtypages, page->getParentId(), const_cast<Field *>(field));
    BTreeEntry insertEntry{const_cast<Field *>(&up.getField(keyField)), const_cast<BTreePageId *>(&page->getId()),
                           const_cast<BTreePageId *>(&newPage->getId())};
    parentPage->insertEntry(insertEntry);
    //3. 设置节点间的关系
    // page newPage rightSibling
    if(page->getRightSiblingId()){
        auto right = (BTreeLeafPage*) getPage(tid, dirtypages, page->getRightSiblingId(), Permissions::READ_WRITE);
        right->setLeftSiblingId(const_cast<BTreePageId *>(&newPage->getId()));
        dirtypages[&right->getId()] = right;
    }
    newPage->setRightSiblingId(page->getRightSiblingId());
    newPage->setLeftSiblingId(const_cast<BTreePageId *>(&page->getId()));
    page->setRightSiblingId(const_cast<BTreePageId *>(&newPage->getId()));

    page->setParentId(const_cast<BTreePageId *>(&newPage->getId()));
    newPage->setParentId(const_cast<BTreePageId *>(&newPage->getId()));

    //4. 增加脏页
    dirtypages[&parentPage->getId()] = parentPage;
    dirtypages[&page->getId()] = page;
    dirtypages[&newPage->getId()] = newPage;

    //5. 返回要插入field的页
    if (field->compare(Op::GREATER_THAN_OR_EQ, &up.getField(keyField))) {
        return newPage;
    }
    return page;
}

BTreeInternalPage *BTreeFile::splitInternalPage(TransactionId tid, PagesMap &dirtypages, BTreeInternalPage *page,
                                                Field *field) {
    int half = page->getNumEntries()/2;
    auto new_pg = (BTreeInternalPage*) getEmptyPage(tid, dirtypages, BTreePageType::INTERNAL);
    auto it = page->rbegin();
    while(it != page->rend() && half > 0){
        auto &tup = *it;
        page->deleteKeyAndRightChild(&tup);
        new_pg->insertEntry(tup);
        half--;
        ++it;
    }

    // insert into parent node
    auto &up = *it;
    page->deleteKeyAndRightChild(&up);
    up.setLeftChild(const_cast<BTreePageId *>(&page->getId()));
    up.setRightChild(const_cast<BTreePageId *>(&new_pg->getId()));

    // parent node may also split
    auto prt_page = getParentWithEmptySlots(tid, dirtypages, page->getParentId(), field);
    prt_page->insertEntry(up);
    page->setParentId(&prt_page->getId());
    new_pg->setParentId(&prt_page->getId());

    //3. 设置newPage子节点的父节点指向
    updateParentPointers(tid, dirtypages, new_pg);

    //4. 增加脏页
    dirtypages[&prt_page->getId()] = prt_page;
    dirtypages[&new_pg->getId()] = new_pg;
    dirtypages[&page->getId()] =page;
    //5. 返回要插入field的页
    if (field->compare(Op::GREATER_THAN_OR_EQ, up.getKey())) {
        return new_pg;
    }
    return page;
}

void BTreeFile::stealFromLeafPage(BTreeLeafPage *page, BTreeLeafPage *sibling, BTreeInternalPage *parent,
                                  BTreeEntry *entry, bool isRightSibling) {
    // TODO pa2.4: implement (BONUS)
}

void BTreeFile::stealFromLeftInternalPage(TransactionId tid, PagesMap &dirtypages, BTreeInternalPage *page,
                                          BTreeInternalPage *leftSibling, BTreeInternalPage *parent,
                                          BTreeEntry *parentEntry) {
    // TODO pa2.4: implement (BONUS)
}

void BTreeFile::stealFromRightInternalPage(TransactionId tid, PagesMap &dirtypages, BTreeInternalPage *page,
                                           BTreeInternalPage *rightSibling, BTreeInternalPage *parent,
                                           BTreeEntry *parentEntry) {
    // TODO pa2.4: implement (BONUS)
}

void BTreeFile::mergeLeafPages(TransactionId tid, PagesMap &dirtypages, BTreeLeafPage *leftPage,
                               BTreeLeafPage *rightPage, BTreeInternalPage *parent, BTreeEntry *parentEntry) {
    // TODO pa2.4: implement (BONUS)
}

void BTreeFile::mergeInternalPages(TransactionId tid, PagesMap &dirtypages, BTreeInternalPage *leftPage,
                                   BTreeInternalPage *rightPage, BTreeInternalPage *parent, BTreeEntry *parentEntry) {
    // TODO pa2.4: implement (BONUS)
}
