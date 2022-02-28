from stdlib.string cimport Str



cdef extern from * nogil:
    """
    #include <fmt/format.h>
    #include <string_view>

    template <typename T, typename S>
    struct Cy_Str_Adapter {
        using type = T;
        constexpr static auto adapt(T t) { return t; }
    };

    template <typename T>
    struct Cy_Str_Adapter<T*, T> {
        using type = std::string_view;
        static std::string_view adapt(T* t) {
            return t->operator std::string_view();
        }
    };

    template <typename S, typename... T>
    S * format(fmt::format_string<typename Cy_Str_Adapter<T, S>::type...> fmt, T... args) {
        S *s = new S();
        s->_str = fmt::format(fmt, Cy_Str_Adapter<T, S>::adapt(args)...);
        return s;
    }
    """

    Str format "format<Cy_Str>" (const char *, ...) except +
