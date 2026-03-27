#pragma once
// loser_tree.h — Tournament tree for k-way sorted merge.
//
// Implements a winner tournament tree: tree_[1] always holds the index of the
// globally smallest cursor. Each pop() costs O(log K) comparisons.

#include <cstdint>
#include <cstddef>
#include <vector>

// A cursor into one sorted run inside a set of column arrays.
// Events within a run are sorted ascending by (frame, scan, tof).
struct RunCursor {
    const uint32_t* frames;
    const uint32_t* scans;
    const uint32_t* tofs;
    const uint32_t* intensities;
    size_t pos;
    size_t end;

    bool     exhausted()  const noexcept { return pos >= end; }
    uint32_t frame()      const noexcept { return frames[pos]; }
    uint32_t scan()       const noexcept { return scans[pos]; }
    uint32_t tof()        const noexcept { return tofs[pos]; }
    uint32_t intensity()  const noexcept { return intensities[pos]; }
    void     advance()          noexcept { ++pos; }
};

// Winner tournament tree over K RunCursors.
//
// Layout (segment tree, 1-based):
//   tree_[1]          — index of globally minimum cursor
//   tree_[2..K_-1]    — internal nodes
//   tree_[K_..2K_-1]  — leaf slots  (cursor index, or sentinel K_ if exhausted)
//
// K_ is the smallest power of 2 >= number of cursors.
// The sentinel value K_ represents +infinity (exhausted cursor).
class TournamentTree {
    int                  K_;
    std::vector<RunCursor> cursors_;
    std::vector<int>     tree_;

    bool lt(int a, int b) const noexcept {
        if (a == K_) return false;  // sentinel is +infinity
        if (b == K_) return true;
        const RunCursor& ca = cursors_[a];
        const RunCursor& cb = cursors_[b];
        if (ca.frame() != cb.frame()) return ca.frame() < cb.frame();
        if (ca.scan()  != cb.scan())  return ca.scan()  < cb.scan();
        return ca.tof() < cb.tof();
    }

    // Recompute internal nodes from leaf_pos up to root.
    void rebuild(int leaf_pos) noexcept {
        for (int p = leaf_pos >> 1; p >= 1; p >>= 1)
            tree_[p] = lt(tree_[2*p], tree_[2*p+1]) ? tree_[2*p] : tree_[2*p+1];
    }

public:
    explicit TournamentTree(std::vector<RunCursor>&& cursors)
        : cursors_(std::move(cursors))
    {
        int n = (int)cursors_.size();
        K_ = 1;
        while (K_ < n) K_ <<= 1;

        tree_.assign(2 * K_, K_);  // fill with sentinel

        for (int i = 0; i < n; ++i)
            tree_[K_ + i] = cursors_[i].exhausted() ? K_ : i;

        // Build internal nodes bottom-up.
        for (int p = K_ - 1; p >= 1; --p)
            tree_[p] = lt(tree_[2*p], tree_[2*p+1]) ? tree_[2*p] : tree_[2*p+1];
    }

    // True when all cursors are exhausted.
    bool empty() const noexcept { return tree_[1] == K_; }

    // Reference to the current minimum cursor.
    RunCursor& top() noexcept { return cursors_[tree_[1]]; }

    // Advance the minimum cursor and restore the heap invariant.
    void pop() noexcept {
        int w        = tree_[1];
        int leaf_pos = K_ + w;
        cursors_[w].advance();
        tree_[leaf_pos] = cursors_[w].exhausted() ? K_ : w;
        rebuild(leaf_pos);
    }
};
